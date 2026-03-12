#!/usr/bin/env python3
"""
Backend Flask — Sistema CFDI
Endpoints:
  POST /api/generar             → genera XMLs desde Excel, sella y timbra
  POST /api/reprocesar          → reprocesa un CFDI por nombre_archivo
  GET  /api/logs/<job_id>       → SSE logs en tiempo real
  GET  /api/historial           → últimos 100 registros de la BD
  GET  /api/cfdi/<nombre>       → detalle de un CFDI

Uso:
  pip install flask flask-cors python-dotenv
  python flask_app.py
"""

import json, os, queue, sys, threading, traceback, uuid
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
import re
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.SelloXML    import SelloXML
from data.PACTimbrador import PACTimbrador
from data.funcionesPostgres import conexion
from data.GeneraXML   import ExcelToNominaXML

app = Flask(__name__)
CORS(app)

# ── Config desde .env ─────────────────────────────────────────────────────────
def _env(k, d=""): return os.getenv(k, d)

WSDL_TEST = _env("PAC_WSDL_TEST")
WSDL_PROD = _env("PAC_WSDL_PROD")
USR_TEST  = _env("PAC_USR_TEST")
USR_PROD  = _env("PAC_USR_PROD")
PWD_TEST  = _env("PAC_PWD_TEST")
PWD_PROD  = _env("PAC_PWD_PROD")

KEY_FILE  = _env("KEY_FILE")
CER_FILE  = _env("CER_FILE")
XSLT_FILE = _env("LIBRERIA_XSLT")
KEY_PASS  = _env("KEY_PASSWORD")

CARPETA_XMLS_PROD  = _env("CARPETA_XMLS_PROD",  "tmp/PROD/XMLs")
CARPETA_XMLS_TEST  = _env("CARPETA_XMLS_TEST",  "tmp/TEST/XMLs")
CARPETA_EXCEL_PROD = _env("CARPETA_EXCEL_PROD", "tmp/PROD/EXCEL")
CARPETA_EXCEL_TEST = _env("CARPETA_EXCEL_TEST", "tmp/TEST/EXCEL")
def carpeta_xmls(ambiente: str) -> str:
    return CARPETA_XMLS_PROD if ambiente == "PROD" else CARPETA_XMLS_TEST

def carpeta_excel(ambiente: str) -> str:
    return CARPETA_EXCEL_PROD if ambiente == "PROD" else CARPETA_EXCEL_TEST
def creds_pac(ambiente: str) -> tuple:
    """Devuelve (wsdl, usr, pwd) según el ambiente."""
    if ambiente == "PROD":
        return WSDL_PROD, USR_PROD, PWD_PROD
    return WSDL_TEST, USR_TEST, PWD_TEST
log_queues: dict[str, queue.Queue] = {}

# ─────────────────────────────────────────────────────────────────────────────
# Worker: GENERAR + SELLAR + TIMBRAR (flujo completo desde Excel)
# ─────────────────────────────────────────────────────────────────────────────
def _worker_generar(job_id: str, excel_path: str, ambiente: str = "TEST"):
    q = log_queues[job_id]
    resumen = {"ok": 0, "fail": 0, "total": 0}

    def log(nivel, msg):
        q.put({"nivel": nivel, "msg": msg, "ts": datetime.now().strftime("%H:%M:%S")})
        print(f"[{nivel}] {msg}")

    try:
        if not os.path.exists(excel_path):
            log("ERROR", f"No se encontró el archivo Excel: {excel_path}"); return

        log("INFO", f"Leyendo Excel: {excel_path}  [Ambiente: {ambiente}]")
        carpeta = Path(carpeta_xmls(ambiente))
        nomina  = ExcelToNominaXML(excel_path, str(carpeta), ambiente=ambiente)
        archivos = nomina.procesar()
        resumen["total"] = len(archivos)
        log("OK", f"{len(archivos)} XML(s) generado(s)")

        if not archivos:
            log("WARN", "No se generaron XMLs. Verifica que el Excel tenga datos."); return

        log("INFO", "Iniciando sellado y timbrado...")
        ok = fail = 0

        with conexion() as db:
            sello = SelloXML(KEY_FILE, KEY_PASS, XSLT_FILE, CER_FILE)
            wsdl, usr, pwd = creds_pac(ambiente)

            log("INFO", f"########################### {wsdl}, {usr}, {pwd}")
            pac   = PACTimbrador(wsdl, "", usr, pwd, db)

            for archivo in archivos:
                xml_in = Path(archivo)
                nombre = xml_in.name

                ruta_sel = str(xml_in.parent).replace("CFDI", "CFDIs_SELLADOS")
                os.makedirs(ruta_sel, exist_ok=True)
                xml_sellado = os.path.join(ruta_sel, f"{xml_in.stem}_SELLADO.xml")

                try:
                    log("INFO", f"Sellando: {nombre}")
                    sello.sellar_xml(str(xml_in), xml_sellado)
                    db.update(
                        {"ruta_sellado": xml_sellado, "sellado": True,
                         "fecha_sellado": datetime.now(), "ultima_actualizacion": datetime.now()},
                        "nomina.procesos_cfdi", "nombre_archivo = %s", (nombre,))

                    ruta_tim = str(xml_in.parent).replace("CFDI", "CFDIs_TIMBRADOS")
                    os.makedirs(ruta_tim, exist_ok=True)

                    log("INFO", f"Timbrando: {nombre}")
                    resp = pac.timbrar_y_guardar(xml_sellado, nombre, base_output_dir=ruta_tim)

                    if resp.get("ok"):
                        log("OK", f"UUID: {resp.get('uuid')}  ← {nombre}")
                        ok += 1
                    else:
                        log("ERROR", f"PAC rechazó [{resp.get('codigo')}]: {resp.get('mensaje')}  ← {nombre}")
                        fail += 1

                except Exception:
                    err = traceback.format_exc()
                    db.update(
                        {"sellado": False, "error_sellado": err, "ultima_actualizacion": datetime.now()},
                        "nomina.procesos_cfdi", "nombre_archivo = %s", (nombre,))
                    log("ERROR", f"Excepción en {nombre}: {err}")
                    fail += 1

        resumen["ok"]   = ok
        resumen["fail"] = fail
        log("OK" if fail == 0 else "WARN",
            f"Proceso terminado — Total: {len(archivos)}  OK: {ok}  FAIL: {fail}")

    except Exception:
        log("ERROR", traceback.format_exc())
    finally:
        q.put({"nivel": "DONE", "resultado": resumen})


# ─────────────────────────────────────────────────────────────────────────────
# Worker: REPROCESAR un solo CFDI
# ─────────────────────────────────────────────────────────────────────────────
def _worker_reprocesar(job_id: str, nombre_archivo: str):
    q = log_queues[job_id]
    resultado = {"ok": False, "uuid": None, "id_bd": None, "codigo": None, "mensaje": None}

    def log(nivel, msg):
        q.put({"nivel": nivel, "msg": msg, "ts": datetime.now().strftime("%H:%M:%S")})
        print(f"[{nivel}] {msg}")

    try:
        log("INFO", f"Reprocesando: {nombre_archivo}")

        with conexion() as db:
            rows = db.query_simple(
                "SELECT id, ruta_xml, sellado, timbrado, uuid, ambiente FROM nomina.procesos_cfdi "
                "WHERE nombre_archivo = %s ORDER BY id DESC LIMIT 1",
                (nombre_archivo,))

            if not rows:
                log("ERROR", f"No encontrado en BD: {nombre_archivo}"); return

            row = rows[0]; _id = row["id"]
            amb = (row.get("ambiente") or "TEST").upper()
            log("INFO", f"Registro encontrado — id={_id}  ambiente={amb}")

            if row.get("timbrado") and row.get("uuid"):
                log("WARN", f"Ya timbrado — UUID: {row['uuid']}")
                resultado = {"ok": True, "uuid": row["uuid"], "id_bd": _id,
                             "codigo": "YA_TIMBRADO", "mensaje": "Ya timbrado"}
                return

            ruta_actual = row.get("ruta_xml")
            if not ruta_actual or not os.path.exists(ruta_actual):
                log("ERROR", f"XML no existe en disco: {ruta_actual}"); return

            xml_in = Path(ruta_actual)
            xml_sellado = str(xml_in)
            if("CFDIs_SELLADOS" not in str(xml_in.parent)):
                ruta_sel = str(xml_in.parent).replace("CFDI", "CFDIs_SELLADOS")
                os.makedirs(ruta_sel, exist_ok=True)
                nombre_limpio = re.sub(r'ID\d+_', '', xml_in.stem).replace("_SELLADO", "")
                xml_sellado = os.path.join(ruta_sel, f"{nombre_limpio}_SELLADO.xml")
            log("INFO", "Sellando XML...")
            sello = SelloXML(KEY_FILE, KEY_PASS, XSLT_FILE, CER_FILE)
            sello.sellar_xml(str(xml_in), xml_sellado)
            log("OK", "XML sellado")
            db.update(
                {"ruta_sellado": xml_sellado, "sellado": True, "error_sellado": None,
                 "fecha_sellado": datetime.now(), "ultima_actualizacion": datetime.now()},
                "nomina.procesos_cfdi", "id = %s", (_id,))
            ruta_tim = str(xml_in.parent)
            if("CFDIs_TIMBRADOS" not in str(xml_in.parent)):
                ruta_tim = str(xml_in.parent).replace("/CFDI", "/CFDIs_TIMBRADOS")
            os.makedirs(ruta_tim, exist_ok=True)

            log("INFO", "Enviando al PAC...")
            wsdl, usr, pwd = creds_pac(amb)
            log("INFO", f"{wsdl}, {usr}, {pwd}")
            pac  = PACTimbrador(wsdl, "", usr, pwd, db)
            resp = pac.timbrar_y_guardar(xml_sellado, nombre_archivo, base_output_dir=ruta_tim)

            if resp.get("ok"):
                log("OK", f"Timbrado — UUID: {resp.get('uuid')}")
                resultado = {"ok": True, "uuid": resp.get("uuid"), "id_bd": _id,
                             "codigo": resp.get("codigo"), "mensaje": resp.get("mensaje")}
            else:
                log("ERROR", f"PAC rechazó [{resp.get('codigo')}]: {resp.get('mensaje')}")
                resultado = {"ok": False, "uuid": None, "id_bd": _id,
                             "codigo": resp.get("codigo"), "mensaje": resp.get("mensaje")}

    except Exception:
        log("ERROR", traceback.format_exc())
    finally:
        q.put({"nivel": "DONE", "resultado": resultado})


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────
def _launch(worker_fn, *args):
    jid = str(uuid.uuid4())
    log_queues[jid] = queue.Queue()
    threading.Thread(target=worker_fn, args=(jid, *args), daemon=True).start()
    return jid

@app.post("/api/generar")
def api_generar():
    # Recibe multipart/form-data con el archivo excel y el campo ambiente
    excel_file = request.files.get("excel")
    ambiente   = (request.form.get("ambiente") or "TEST").strip().upper()
    if ambiente not in ("TEST", "PROD"):
        ambiente = "TEST"

    if not excel_file or not excel_file.filename:
        return jsonify({"error": "Se requiere el archivo Excel"}), 400

    # Guardar el Excel en la carpeta correspondiente al ambiente
    destino = Path(carpeta_excel(ambiente))
    destino.mkdir(parents=True, exist_ok=True)

    # Evitar sobreescrituras: agregar timestamp al nombre
    nombre_original = Path(excel_file.filename).stem
    extension       = Path(excel_file.filename).suffix
    timestamp       = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_guardado = f"{nombre_original}_{timestamp}{extension}"
    excel_path      = str(destino / nombre_guardado)

    excel_file.save(excel_path)
    return jsonify({"job_id": _launch(_worker_generar, excel_path, ambiente)})

@app.post("/api/reprocesar")
def api_reprocesar():
    data   = request.get_json(silent=True) or {}
    nombre = data.get("nombre_archivo", "").strip()
    if not nombre:
        return jsonify({"error": "nombre_archivo requerido"}), 400
    return jsonify({"job_id": _launch(_worker_reprocesar, nombre)})

@app.get("/api/logs/<job_id>")
def api_logs(job_id):
    if job_id not in log_queues:
        return jsonify({"error": "job no encontrado"}), 404
    def stream():
        q = log_queues[job_id]
        while True:
            try:
                ev = q.get(timeout=30)
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("nivel") == "DONE": break
            except queue.Empty:
                yield 'data: {"nivel":"PING"}\n\n'
    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/api/historial")
def api_historial():
    try:
        with conexion() as db:
            rows = db.query_simple(
                "SELECT id, nombre_archivo, rfc_emisor, rfc_receptor, folio, "
                "generado, sellado, timbrado, cancelado, uuid, codigo_pac, mensaje_pac, "
                "fecha_generado, fecha_sellado, fecha_timbrado, fecha_cancelacion, "
                "error_sellado, ruta_xml, ruta_xml_timbrado, ruta_sellado, ultima_actualizacion, ambiente "
                "FROM nomina.procesos_cfdi ORDER BY ultima_actualizacion DESC LIMIT 100")
            def s(v): return v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v
            return jsonify([{k: s(v) for k, v in r.items()} for r in rows])
    except Exception:
        return jsonify({"error": traceback.format_exc()}), 500

@app.get("/api/cfdi/<nombre>")
def api_cfdi(nombre):
    try:
        with conexion() as db:
            rows = db.query_simple(
                "SELECT * FROM nomina.procesos_cfdi WHERE nombre_archivo = %s ORDER BY id DESC LIMIT 1",
                (nombre,))
            if not rows: return jsonify({"error": "No encontrado"}), 404
            def s(v): return v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v
            return jsonify({k: s(v) for k, v in rows[0].items()})
    except Exception:
        return jsonify({"error": traceback.format_exc()}), 500

if __name__ == "__main__":
    print("\n  API CFDI → http://localhost:5000\n")
    app.run(debug=True, threaded=True, port=5000)