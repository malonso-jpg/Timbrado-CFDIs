import os
import re
import hashlib
from pathlib import Path
import requests
from lxml import etree
from zeep import Client
from zeep.transports import Transport
from zeep.exceptions import Fault
from zeep.plugins import HistoryPlugin
from datetime import datetime
import traceback

class PACTimbrador:

    def __init__(self, wsdl_url, rfc, usuario, password, con, timeout = 20, verify_tls = True):
        self.wsdl_url = wsdl_url
        self.rfc = rfc
        self.usuario = usuario
        self.password = password
        self.con = con
        self.timeout = timeout
        self.verify_tls = verify_tls

    @staticmethod
    def password_md5(text: str) -> str:
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _to_bytes(resp) -> bytes:
        if resp is None:
            return b""
        if isinstance(resp, bytes):
            return resp
        if isinstance(resp, str):
            return resp.encode("utf-8", errors="replace")
        return bytes(resp)

    @staticmethod
    def _parse_respuesta_pac(resp_bytes: bytes) -> dict:
        """
        Devuelve:
        {
          "codigo": "10",
          "mensaje": "Correcto",
          "cfdi_text": "<cfdi:Comprobante ...>...</cfdi:Comprobante>" or None,
          "raw_text": "<RespuestaPAC>...</RespuestaPAC>"
        }
        """
        raw_text = resp_bytes.decode("utf-8", errors="replace").strip()

        # Algunos PAC devuelven espacios/encabezados antes del XML; intenta encontrar '<RespuestaPAC'
        idx = raw_text.find("<RespuestaPAC")
        if idx > 0:
            raw_text = raw_text[idx:]

        try:
            root = etree.fromstring(raw_text.encode("utf-8"))
        except Exception:
            # Si no es XML válido, regresamos raw
            return {
                "codigo": None,
                "mensaje": None,
                "cfdi_text": None,
                "raw_text": raw_text,
                "parse_error": "RESPUESTA_NO_ES_XML",
            }

        def get_text(tag: str):
            el = root.find(f".//{tag}")
            return (el.text or "").strip() if el is not None else None

        codigo = get_text("Codigo")
        mensaje = get_text("Mensaje")
        cfdi_node = root.find(".//CFDI")

        cfdi_text = None
        if cfdi_node is not None:
            # Con CDATA, lxml lo deja en .text
            val = (cfdi_node.text or "").strip()
            if val and val.upper() != "NA":
                cfdi_text = val

        return {
            "codigo": codigo,
            "mensaje": mensaje,
            "cfdi_text": cfdi_text,
            "raw_text": raw_text,
            "parse_error": None,
        }

    def _append_log_csv(self, csv_path, name, row):
        datos_update = {
            "timbrado": True,
            "mensaje_pac": row.get("mensaje"),
            "codigo_pac": row.get("codigo"),
            "fecha_timbrado": datetime.now(),
            "uuid": row.get("uuid"),
            "ruta_xml_timbrado": row.get("ruta_xml")

        }
        if(row.get("codigo") != "10"):
            datos_update = {
                "timbrado": False,
                "mensaje_pac": row.get("mensaje"),
                "codigo_pac": row.get("codigo"),
                "ruta_xml_timbrado": row.get("ruta_xml", "")
            }
        datos_update['ultima_actualizacion'] = datetime.now()
        self.con.update(datos_update, "nomina.procesos_cfdi", "nombre_archivo = %s", (name,))
    @staticmethod
    def _soap_escape(s: str) -> str:
        return (s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;"))

    def _cancelarCFDI_raw(self, xml_bytes: bytes, pfx_bytes: bytes, password_pfx: str) -> bytes:
        """
        Llama cancelarCFDI con SOAP crudo (sin zeep) para evitar UnicodeDecodeError
        cuando el PAC regresa acuse binario.
        Regresa el SOAP response completo en bytes.
        """
        import base64
        import requests

        endpoint = self.wsdl_url.split("?wsdl")[0]
        xml_b64 = base64.b64encode(xml_bytes).decode("ascii")
        pfx_b64 = base64.b64encode(pfx_bytes).decode("ascii")

        password_md5 = self.password_md5(self.password)

        soap = f"""<?xml version="1.0" encoding="UTF-8"?>
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                            xmlns:ser="http://servicio.cancela.ws.sto.pac.com/">
            <soapenv:Header/>
            <soapenv:Body>
                <ser:cancelarCFDI>
                <usuario>{self._soap_escape(self.usuario)}</usuario>
                <password>{self._soap_escape(password_md5)}</password>
                <cfdi>{xml_b64}</cfdi>
                <pfx>{pfx_b64}</pfx>
                <passwordPFX>{self._soap_escape(password_pfx)}</passwordPFX>
                </ser:cancelarCFDI>
            </soapenv:Body>
            </soapenv:Envelope>""".encode("utf-8")

        headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": ""}
        print(soap.decode("utf-8", errors="replace"))
        r = requests.post(endpoint, data=soap, headers=headers, timeout=self.timeout, verify=self.verify_tls)
        r.raise_for_status()
        return r.content

    def _extraer_retorno_soap(self, soap_response: bytes) -> bytes:
        """
        Extrae el texto del elemento <return> o <...Result> del SOAP.
        Si es base64, lo decodifica a bytes (acuse binario).
        Si es XML directo, devuelve bytes del XML.
        """
        import base64
        import xml.etree.ElementTree as ET

        root = ET.fromstring(soap_response)

        candidates = []
        for el in root.iter():
            tag = el.tag if isinstance(el.tag, str) else ""
            if tag.endswith("return") or tag.endswith("Result"):
                if el.text and el.text.strip():
                    candidates.append(el.text.strip())

        if not candidates:
            # si no encontramos return, devolvemos todo
            return soap_response

        data = candidates[-1]

        # si parece XML
        if data.lstrip().startswith("<"):
            return data.encode("utf-8", errors="replace")

        # si es base64 (acuse DER/PKCS7)
        try:
            return base64.b64decode(data, validate=False)
        except Exception:
            return data.encode("utf-8", errors="replace")
    def cancelar_por_uuid(self, uuid: str, pfx_ruta: str, password_pfx: str,
                        base_output_dir="tmp/CANCELACIONES"):

        out = {
            "ok": False,
            "codigo": None,
            "mensaje": None,
            "uuid": uuid,
            "ruta_acuse": None,
            "error": None,
        }

        try:
            rows = self.con.query(
                "nombre_archivo, ruta_xml_timbrado",
                "nomina.procesos_cfdi",
                f"uuid = '{uuid}'"
            )

            if not rows:
                out["error"] = f"UUID no encontrado: {uuid}"
                return out

            row = rows[0]
            nombre_archivo = row.get("nombre_archivo")
            ruta_xml = row.get("ruta_xml_timbrado")

        except Exception as e:
            out["error"] = f"Error consultando BD: {e}"
            return out

        if not ruta_xml:
            out["error"] = f"No hay ruta_xml_timbrado para UUID {uuid}"
            return out

        xml_path = Path(ruta_xml)

        if not xml_path.exists():
            out["error"] = f"No existe XML: {ruta_xml}"
            return out

        xml_bytes = xml_path.read_bytes()
        pfx_bytes = Path(pfx_ruta).read_bytes()

        password_md5 = self.password_md5(self.password)
        history = HistoryPlugin()
        try:

            motivo = "02"
            folio_sustitucion = ""  # vacío excepto si motivo = "01"
            session = requests.Session()
            session.verify = self.verify_tls
            transport = Transport(session=session, timeout=self.timeout)

            client = Client(wsdl=self.wsdl_url, transport=transport, plugins=[history])
            resp = client.service.cancelarCFDI(
                self.usuario,
                password_md5,        # ojo: este campo se llama contrasena
                "CAD221214422",        # o tu RFC emisor real (CAD221214422)
                uuid,
                pfx_bytes,                  # bytes OK (base64Binary)
                password_pfx,
                motivo,
                folio_sustitucion
            )

        except Fault as e:
            print("SOAP Fault:", e)
            out["error"] = f"SOAP Fault: {e}"
            self.con.update({
                "cancelado": False,
                "fecha_cancelacion": datetime.now(),
                "mensaje_cancelacion": str(traceback.format_exc()),
                "ultima_actualizacion": datetime.now()
            }, "nomina.procesos_cfdi", "nombre_archivo = %s", (nombre_archivo,))
            return out

        except Exception as e:
            print("Error SOAP:", e)
            out["error"] = f"Error SOAP: {repr(e)}"
            self.con.update({
                "cancelado": False,
                "fecha_cancelacion": datetime.now(),
                "mensaje_cancelacion": str(traceback.format_exc()),
                "ultima_actualizacion": datetime.now()
            }, "nomina.procesos_cfdi", "nombre_archivo = %s", (nombre_archivo,))
            return out

        # 7) Parse respuesta (Zeep normalmente da un objeto con .codEstatus / .mensaje)
        codigo = None
        mensaje = None

        try:
            # caso típico
            codigo = getattr(resp, "codEstatus", None) or getattr(resp, "Codigo", None)
            mensaje = getattr(resp, "mensaje", None) or getattr(resp, "Mensaje", None)

            # si viene anidado (a veces Zeep lo envuelve)
            if codigo is None and isinstance(resp, dict):
                codigo = resp.get("codEstatus") or resp.get("Codigo")
                mensaje = resp.get("mensaje") or resp.get("Mensaje")
        except Exception:
            pass

        out["codigo"] = codigo
        out["mensaje"] = mensaje

        # 8) Guardar acuse SOAP real (response envelope)
        try:
            Path(base_output_dir).mkdir(parents=True, exist_ok=True)
            acuse_path = Path(base_output_dir) / f"{xml_path.stem}_ACUSE_CANCELACION.xml"

            if history.last_received and history.last_received["envelope"] is not None:
                envelope = history.last_received["envelope"]
                acuse_path.write_bytes(etree.tostring(envelope, pretty_print=True, xml_declaration=True, encoding="UTF-8"))
            else:
                # fallback: guarda lo que haya en texto
                acuse_path.write_text(str(resp), encoding="utf-8", errors="ignore")

            out["ruta_acuse"] = str(acuse_path)
        except Exception as e:
            print(traceback.format_exc())
            out["ruta_acuse"] = None
            out["error"] = (out["error"] or "") + f" | No pude guardar acuse: {repr(e)}"

        # 9) Determinar OK
        # En STO, éxito suele venir como "201" o algún estatus de éxito. Ajusta si tu PAC usa otro.
        ok = str(codigo).strip() == "201"
        out["ok"] = ok

        # 10) Update BD
        datos_update = {
            "cancelado": ok,
            "codigo_cancelacion": codigo,
            "mensaje_cancelacion": mensaje,
            "fecha_cancelacion": datetime.now(),
            "ruta_acuse_cancelacion": out["ruta_acuse"],
            "ultima_actualizacion": datetime.now()
        }
        self.con.update(
            datos_update,
            "nomina.procesos_cfdi",
            "nombre_archivo = %s",
            (nombre_archivo,)
        )

        return out
    def timbrar_y_guardar(self, xml_ruta, nombre, base_output_dir = "tmp", bitacora_csv = "tmp/bitacora_xml.csv"):
        out = {
            "ok": False
        }

        xml_path = Path(xml_ruta)
        xml_nombre = xml_path.name

        try:
            xml_bytes = xml_path.read_bytes()
        except Exception as e:
            # Log a CSV también
            self._append_log_csv(bitacora_csv, nombre, {
                "rfc": self.rfc,
                "archivo": xml_nombre,
                "codigo": "LOCAL_READ_ERROR",
                "mensaje": str(traceback.format_exc()),
            })
            return out

        password_md5 = self.password_md5(self.password)

        try:
            session = requests.Session()
            session.verify = self.verify_tls
            transport = Transport(session=session, timeout=self.timeout)
            client = Client(wsdl=self.wsdl_url, transport=transport)

            resp = client.service.timbrarCfdi(self.usuario, password_md5, xml_bytes)

        except Fault as e:
            self._append_log_csv(bitacora_csv, nombre, {
                "rfc": self.rfc,
                "archivo": xml_nombre,
                "codigo": "SOAP_FAULT",
                "mensaje": str(traceback.format_exc()),
            })
            return out

        except Exception as e:
            self._append_log_csv(bitacora_csv, nombre, {
                "rfc": self.rfc,
                "archivo": xml_nombre,
                "codigo": "SOAP_ERROR",
                "mensaje": str(traceback.format_exc()),
            })
            return out

        resp_bytes = self._to_bytes(resp)
        parsed = self._parse_respuesta_pac(resp_bytes)

        out["codigo"] = parsed.get("codigo")
        out["mensaje"] = parsed.get("mensaje")

        # Si no pudimos parsear la respuesta como XML
        if parsed.get("parse_error"):
            out["error"] = parsed["parse_error"]
            self._append_log_csv(bitacora_csv, nombre, {
                "rfc": self.rfc,
                "archivo": xml_nombre,
                "codigo": parsed["parse_error"],
                "mensaje": str(parsed.get("raw_text", "")),
            })
            return out

        # Éxito: codigo 10 y cfdi_text presente
        if out["codigo"] == "10" and parsed.get("cfdi_text"):
            os.makedirs(base_output_dir, exist_ok=True)

            

            # Extraer UUID del CFDI timbrado
            uuid = None
            try:
                cfdi_root = etree.fromstring(parsed["cfdi_text"].encode("utf-8"))
                tfd = cfdi_root.find(".//{http://www.sat.gob.mx/TimbreFiscalDigital}TimbreFiscalDigital")
                if tfd is not None:
                    uuid = tfd.get("UUID")
            except Exception as e:
                self._append_log_csv(bitacora_csv, nombre, {
                    "rfc": self.rfc,
                    "archivo": xml_nombre,
                    "codigo": "UUID_PARSE_ERROR",
                    "mensaje": str(traceback.format_exc()),
                })

            salida_nombre = f"{uuid.upper()}.xml" if uuid else xml_nombre.replace(".xml", "_timbrado.xml")
            ruta_xml = os.path.join(base_output_dir, salida_nombre)

            Path(ruta_xml).write_text(parsed["cfdi_text"], encoding="utf-8")
            self._append_log_csv(bitacora_csv, nombre,{
                "rfc": self.rfc,
                "archivo": xml_nombre,
                "estatus": "OK",
                "codigo": out["codigo"],
                "mensaje": "",
                "uuid": uuid,
                "ruta_xml": ruta_xml,
            })

            out["ok"] = True
            out["uuid"] = uuid
            out["ruta_xml_timbrado"] = ruta_xml
            return out

        # Opcional: guardar la respuesta completa para auditoría
        ruta_err = base_output_dir.replace("CFDIs_TIMBRADOS", "CFDIs_ERRORES")
        os.makedirs(ruta_err, exist_ok=True)
        ruta_resp = os.path.join(ruta_err, xml_nombre.replace(".xml", "_respuesta_pac.xml"))
        Path(ruta_resp).write_text(parsed.get("raw_text", ""), encoding="utf-8")
        out["ruta_respuesta_guardada"] = ruta_resp

        # Error (incluye CFDI=NA)
        self._append_log_csv(bitacora_csv, nombre, {
            "rfc": self.rfc,
            "archivo": ruta_resp,
            "codigo": out["codigo"] or "",
            "mensaje": out["mensaje"] or "",
        })
        out["ok"] = False
        return out
'''
SERVICE: CancelaImplService
  PORT: CancelaImplPort
    OP: documentosRelacionados
      IN : usuario: xsd:string, contrasena: xsd:string, rfcReceptor: xsd:string, uuid: xsd:string, pfx: xsd:base64Binary, contrasenaPfx: xsd:string
      OUT: DocumentosRelacionadosRespuesta: ns0:consultaRelacionadosResponseDTO
    OP: aceptacionRechazo
      IN : usuario: xsd:string, contrasena: xsd:string, rfcReceptor: xsd:string, uuid: xsd:string, operacion: xsd:boolean, pfx: xsd:base64Binary, contrasenaPfx: xsd:string
      OUT: AceptaRechazoRespuesta: ns0:aceptaRechazaResponseDTO
    OP: consultaEstatus
      IN : usuario: xsd:string, contrasena: xsd:string, rfcEmisor: xsd:string, rfcReceptor: xsd:string, uuid: xsd:string, total: xsd:string
      OUT: ConsultaEstatusRespuesta: ns0:consultaEstatusResponseDTO
    OP: consultaPendientes
      IN : usuario: xsd:string, contrasena: xsd:string, rfcReceptor: xsd:string, pfx: xsd:base64Binary, contrasenaPfx: xsd:string
      OUT: PendientesRespuesta: ns0:pendientesResponseDTO
    OP: cancelarCFDI
      IN : usuario: xsd:string, contrasena: xsd:string, rfcEmisor: xsd:string, uuid: xsd:string, pfx: xsd:base64Binary, contrasenaPfx: xsd:string, motivo: xsd:string, folioSustitucion: xsd:string
      OUT: CancelacionPACRespuesta: ns0:cancelaDocumentoResponseDTO
'''