from zeep.plugins import HistoryPlugin
from lxml import etree
from zeep import Client
from zeep.transports import Transport
import requests

session = requests.Session()
transport = Transport(session=session)

history = HistoryPlugin()
client = Client(wsdl="https://pac-test.stofactura.com/pac-sto-ws/cfdi33?wsdl", transport=transport, plugins=[history])

# ... llamas cancelarCFDI ...

sent = etree.tostring(history.last_sent["envelope"], pretty_print=True).decode("utf-8")
print("SOAP ENVIADO:\n", sent)

received = etree.tostring(history.last_received["envelope"], pretty_print=True).decode("utf-8")
print("SOAP RECIBIDO:\n", received)