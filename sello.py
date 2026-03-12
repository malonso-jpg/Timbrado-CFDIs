import base64
from pathlib import Path
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_der_private_key, load_pem_private_key
from cryptography.hazmat.primitives.asymmetric import padding
from saxonche import PySaxonProcessor
from lxml import etree
import re

XML_IN = "//home/malonso/Descargas/CFDI_SEBE070123QJ3_8844471_20260224_124302.xml"
XML_OUT = "/home/malonso/Documentos/Proyectos/TIMBRADO_CFDI/tmp/prueba_sellada.xml"

XSLT = "xslt_sat/cadenaoriginal_4_0.xslt"

KEY_FILE = "/home/malonso/Descargas/CSD_CAD221214422_20230726140448-20240723T201117Z-001/CSD_CAD221214422_20230726140448/CSD_CAAFU_ALTO_DESARROLLO_SA_DE_CV_CAD221214422_20230726_140334.key"
KEY_PASSWORD = b"21Oremoh"  # bytes

def cadena_original_sin_riesgo(raw: str) -> str:
    # Saxon a veces mete \n. Quita SOLO \r y \n.
    return raw.replace("\r", "").replace("\n", "").strip()
def limpiar_cadena_sat(raw: str) -> str:
    s = raw.replace("\r", "").strip()
    # Saxon suele meter '\n' aunque la cadena no debería llevarlos:
    s = s.replace("\n", "")
    return s
def normalizar_cadena_sat(raw: str) -> str:
    # 1) quita CR/LF
    s = raw.replace("\r", "").replace("\n", "").strip()

    # 2) SAT siempre usa || al inicio y al final.
    # Si la salida trae cosas antes/después, nos quedamos con el bloque ||...||
    m = re.search(r"\|\|.*\|\|", s)
    if m:
        return m.group(0)

    # 3) si no trae ||, pero trae muchos pipes, al menos forzamos bordes
    if s.startswith("|") and not s.startswith("||"):
        s = "|" + s
    if s.endswith("|") and not s.endswith("||"):
        s = s + "|"

    return s
def generar_cadena_original(xml_path: str, xslt_path: str) -> str:
    with PySaxonProcessor(license=False) as proc:
        xslt30 = proc.new_xslt30_processor()
        exe = xslt30.compile_stylesheet(stylesheet_file=xslt_path)
        raw = exe.transform_to_string(source_file=xml_path)
        cadena = normalizar_cadena_sat(raw)
        return cadena

def cargar_llave_privada(path: str, password: bytes):
    data = Path(path).read_bytes()
    try:
        return load_der_private_key(data, password=password)
    except Exception:
        return load_pem_private_key(data, password=password)

def firmar_cadena(private_key, cadena_original: str) -> str:
    firma = private_key.sign(
        cadena_original.encode("utf-8"),
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    return base64.b64encode(firma).decode("ascii")

def insertar_sello_en_xml(xml_in: str, xml_out: str, sello_b64: str):
    parser = etree.XMLParser(remove_blank_text=False)
    tree = etree.parse(xml_in, parser)
    root = tree.getroot()

    # Debe ser cfdi:Comprobante
    # Pon/actualiza el atributo Sello
    root.set("Sello", sello_b64)

    tree.write(xml_out, encoding="UTF-8", xml_declaration=True)

def main():
    cadena = generar_cadena_original(XML_IN, XSLT)
    print("CADENA ORIGINAL:")
    print(cadena)
    print("LEN:", len(cadena))

    priv = cargar_llave_privada(KEY_FILE, KEY_PASSWORD)
    sello = firmar_cadena(priv, cadena)

    print("\nSELLO (base64):")
    print(sello)

    insertar_sello_en_xml(XML_IN, XML_OUT, sello)
    print("\nOK: XML sellado en:", XML_OUT)

if __name__ == "__main__":
    main()