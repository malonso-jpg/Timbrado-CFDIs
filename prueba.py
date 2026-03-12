from zeep import Client
from zeep.transports import Transport
import hashlib
import requests

# WSDL STO PAC
WSDL = "https://pac-test.stofactura.com/pac-sto-ws/cfdi33?wsdl"

USER = "TESTUSERSTO"
PASSWORD = "TESTPASSSTO"
PFX_PASS = "21Oremoh"

XML_FILE = "/home/malonso/Documentos/Proyectos/TIMBRADO_CFDI/tmp/XMLs/CAD221214422/CFDIs_TIMBRADOS/78FEFCD0-D3D9-420F-986A-C3FF076DD332.xml"
PFX_FILE = "/home/malonso/Descargas/CSD_CAD221214422_20230726140448-20240723T201117Z-001/CSD_CAD221214422_20230726140448/certificado_legacy.pfx"

def md5(text):
    return hashlib.md5(text.encode()).hexdigest()


# leer archivos
with open(XML_FILE, "rb") as f:
    xml_bytes = f.read()

with open(PFX_FILE, "rb") as f:
    pfx_bytes = f.read()

# cliente SOAP
session = requests.Session()
transport = Transport(session=session)

client = Client(WSDL, transport=transport)

try:
    respuesta = client.service.cancelarCfdi(
        USER,
        md5(PASSWORD),   # password en MD5
        xml_bytes,
        pfx_bytes,
        PFX_PASS
    )

    print("RESPUESTA DEL PAC:")
    if isinstance(respuesta, bytes):
        print(respuesta.decode("utf-8"))
    else:
        print(respuesta)

except Exception as e:
    print("ERROR:")
    print(e)