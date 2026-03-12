#!/usr/bin/env python3
from pathlib import Path

from pathlib import Path
from datetime import datetime
import traceback

from data.SelloXML import SelloXML
from data.PACTimbrador import PACTimbrador
from data.funcionesPostgres import conexion
from data.GeneraXML import ExcelToNominaXML
import os

WSDL_TEST = "https://pac-test.stofactura.com/pac-sto-ws/cfdi33?wsdl"
CARPETA_XMLS = Path("tmp/XMLs")

KEY_FILE = Path("tmp/FIRMAS/FTO210226USA/CSD_FPT_Corporativo_FTO210226USA_20260210_112135.key")
CER_FILE = Path("tmp/FIRMAS/FTO210226USA/00001000000722317463.cer")
XSLT_FILE = Path("cadenaoriginal_4_0.xslt")  # si está en otro folder pon ruta absoluta
KEY_PASSWORD = "FTO210226"
PAC_SUC = "FTO210226USA"
PAC_USR = "TESTUSERSTO"
PAC_PWD = "TESTPASSSTO"


def main():
    ARCHIVO_EXCEL = 'tmp/TEMPLATE_3.xlsx'
    CARPETA_XMLS = Path("tmp/XMLs")
    nomina = ExcelToNominaXML(ARCHIVO_EXCEL, CARPETA_XMLS)
    archivos = nomina.procesar()

    ok, fail = 0, 0
    with conexion() as db:
        sello = SelloXML(str(KEY_FILE), KEY_PASSWORD, str(XSLT_FILE), str(CER_FILE))
        pac = PACTimbrador(WSDL_TEST, "", PAC_USR, PAC_PWD, db)
        print(f"4. Iniciando el proceso de sellado y timbrado para {len(archivos)} archivos...")
        for archivo in archivos:
            xml_in = Path(archivo)
            nombre = os.path.basename(xml_in)
            # 1) Sellar
            ruta_sellos = os.path.dirname(xml_in).replace("CFDI", "CFDIs_SELLADOS")
            os.makedirs(ruta_sellos, exist_ok=True)
            xml_sellado = os.path.join(ruta_sellos, f"{xml_in.stem}_SELLADO.xml")
            try:
                sello.sellar_xml(str(xml_in), xml_sellado)
                if(os.path.exists(xml_sellado)):
                    datos_update = {
                        "ruta_xml": xml_sellado,
                        "sellado": True,
                        "fecha_sellado": datetime.now(),
                        "ultima_actualizacion": datetime.now()
                    }
                    
                    db.update(datos_update, "nomina.procesos_cfdi", "nombre_archivo = %s", (nombre,))
                    # 2) Timbrar (tu método ya “guarda”; si devuelve algo, puedes usarlo)
                    # Si tu método timbrar_y_guardar guarda con nombre fijo, mejor que acepte output.
                    ruta = os.path.dirname(xml_in).replace("CFDI", "CFDIs_TIMBRADOS")
                    resp = pac.timbrar_y_guardar(str(xml_sellado), nombre, base_output_dir=ruta)
                    if not resp.get("ok"):
                        fail += 1
                    ok += 1
            except Exception as e:
                datos_update = {
                    "sellado": False,
                    "error_sellado": str(traceback.format_exc()),
                    "ultima_actualizacion": datetime.now()
                }
                db.update(datos_update, "nomina.procesos_cfdi", "nombre_archivo = %s", (nombre,))
                fail += 1
                continue

    print(f"Proceso terminado. Total: {len(archivos)}, OK: {ok}, FAIL: {fail}")


if __name__ == "__main__":
    main()
'''
if __name__ == "__main__":
    
    XML = "/home/malonso/Descargas/generador_XMLs_para_Smartjobs/nominas_v5/CFDI_CALJ0602077S5_Sem11-14_20260226_111553.xml"
    KEY_FILE = "/home/malonso/Descargas/CSD_CAD221214422_20230726140448-20240723T201117Z-001/CSD_CAD221214422_20230726140448/CSD_CAAFU_ALTO_DESARROLLO_SA_DE_CV_CAD221214422_20230726_140334.key"
    CER_FILE = "/home/malonso/Descargas/CSD_CAD221214422_20230726140448-20240723T201117Z-001/CSD_CAD221214422_20230726140448/00001000000701284605.cer"
    XML_SELLO = "tmp/CFDIs_sellados/CFDI_sellado.xml"
    KEY_PASSWORD = "21Oremoh"
    sello = SelloXML(KEY_FILE, KEY_PASSWORD, "cadenaoriginal_4_0.xslt", CER_FILE)
    sello.sellar_xml(XML, XML_SELLO)

    WSDL_TEST = "https://pac-test.stofactura.com/pac-sto-ws/cfdi33?wsdl"
    pac = PACTimbrador(WSDL_TEST, "SAE190", "TESTUSERSTO", "TESTPASSSTO")
    pac.timbrar_y_guardar(XML_SELLO)

    """    
    rfc = "CALJ0602077S5"
    usuario = "TESTUSERSTO"
    password = "TESTPASSSTO"            # el password original (SIN md5)
    xml_ruta = "/home/malonso/Documentos/Proyectos/TIMBRADO_CFDI/tmp/prueba3_sellada.xml"
    main(rfc, usuario, password, xml_ruta)
    """ '''