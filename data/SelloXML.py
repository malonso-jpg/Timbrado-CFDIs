import os
import base64
import requests
from lxml import etree
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import subprocess
import re
class SATResolver(etree.Resolver):
    """
    Resolver que:
      - Evita resolver rutas locales peligrosas
      - Busca imports/includes XSLT en un cache local (xslt_sat/)
      - Si no existe local, intenta descargar desde http(s) y guardarlo
    """

    def __init__(self, cache_dir: str = "xslt_sat", timeout: int = 10):
        super().__init__()
        self.cache_dir = cache_dir
        self.timeout = timeout
        os.makedirs(self.cache_dir, exist_ok=True)

    def resolve(self, url, pubid, context):
        # Ignorar rutas locales para que no intente "abrir" archivos del sistema
        if url.startswith("file://") or url.startswith("/") or ":\\" in url:
            return None

        nombre_archivo = url.split("/")[-1].split("?")[0]
        ruta_local = os.path.join(self.cache_dir, nombre_archivo)

        # 1) Si ya está cacheado, úsalo
        if os.path.exists(ruta_local):
            return self.resolve_filename(ruta_local, context)

        # 2) Si es http(s), descárgalo y cachea
        if url.startswith("http://") or url.startswith("https://"):
            try:
                print(f"[SATResolver] Descargando: {url}")
                r = requests.get(url, timeout=self.timeout)
                r.raise_for_status()
                with open(ruta_local, "wb") as f:
                    f.write(r.content)
                return self.resolve_filename(ruta_local, context)
            except Exception as e:
                print(f"[SATResolver] No pude descargar {url}: {e}")
                return None

        return None

  

class SelloXML:
    def __init__(self, key_der, password, xslt_filename, archivo_cer, xslt_cache_dir = "xslt_sat"):
        self.xslt_cache_dir = xslt_cache_dir
        self.key_der = key_der
        self.password = password
        self.xslt_filename = xslt_filename
        self.archivo_cer = archivo_cer
        os.makedirs(self.xslt_cache_dir, exist_ok=True)
        with open(archivo_cer, 'rb') as f:
            self.certificado_bytes = f.read()
         # Extraer número de certificado
        self.no_certificado = self.extraer_no_certificado_openssl()
        
        # Convertir certificado a base64
        self.certificado_base64 = base64.b64encode(self.certificado_bytes).decode('utf-8')

    def extraer_no_certificado_openssl(self):
        """
        Extrae NoCertificado (20 dígitos) desde .cer usando OpenSSL.
        Convierte el serial HEX a ASCII (forma usada por SAT en CSD).
        """
        p = subprocess.run(
            ["openssl", "x509", "-inform", "DER", "-in", self.archivo_cer, "-noout", "-serial"],
            capture_output=True,
            text=True,
            check=False,
        )
        if p.returncode != 0:
            raise Exception(f"OpenSSL error leyendo CER: {p.stderr.strip() or p.stdout.strip()}")

        m = re.search(r"serial=([0-9A-Fa-f]+)", p.stdout.strip())
        if not m:
            raise Exception(f"No pude leer serial del certificado. Salida: {p.stdout.strip()}")

        serial_hex = m.group(1)

        # HEX -> bytes
        serial_bytes = bytes.fromhex(serial_hex)

        # bytes -> ASCII
        try:
            serial_ascii = serial_bytes.decode("ascii", errors="strict")
        except UnicodeDecodeError:
            # fallback: por si no viene en ASCII (raro), intenta decimal
            serial_ascii = str(int(serial_hex, 16))

        serial_ascii = serial_ascii.strip()

        # Validación típica: 20 dígitos
        if not (len(serial_ascii) == 20 and serial_ascii.isdigit()):
            raise Exception(
                f"NoCertificado no válido obtenido del CER.\n"
                f"serial(hex)={serial_hex}\n"
                f"serial(ascii/dec)={serial_ascii}"
            )
        print(f"NoCertificado extraído: {serial_ascii}")
        return serial_ascii
    # -------------------------
    # 1) Cadena original
    # -------------------------
    def generar_cadena_original(self, xml_path):
        """
        Aplica XSLT al XML para obtener la cadena original.
        xslt_filename debe existir en xslt_sat/ (o traer includes/imports que el resolver descargue).
        """
        resolver = SATResolver(cache_dir=self.xslt_cache_dir)
        parser = etree.XMLParser(remove_blank_text=True, recover=False)
        parser.resolvers.add(resolver)

        # Parse XML
        xml_doc = etree.parse(xml_path, parser)

        # Parse XSLT (binario para respetar encoding)
        xslt_path = os.path.join(self.xslt_cache_dir, self.xslt_filename)
        if not os.path.exists(xslt_path):
            raise FileNotFoundError(f"No existe el XSLT en cache: {xslt_path}")

        with open(xslt_path, "rb") as f:
            xslt_root = etree.parse(f, parser)

        transform = etree.XSLT(xslt_root)
        cadena = str(transform(xml_doc))

        # Normalización mínima (tu puedes ajustar)
        return cadena.replace("\r", "").strip()

    # -------------------------
    # 2) Generar sello
    # -------------------------
    def generar_sello(self, cadena_original):
        """
        Genera el sello (base64) firmando la cadena original con RSA SHA256 (PKCS#1 v1.5).
        archivo_key_der: .key en DER (como CSD del SAT).
        """
        with open(self.key_der, "rb") as f:
            key_data = f.read()

        priv_key = serialization.load_der_private_key(
            key_data,
            password=self.password.encode("utf-8"),
        )

        sello_bytes = priv_key.sign(
            cadena_original.encode("utf-8"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )

        return base64.b64encode(sello_bytes).decode("utf-8")

    # -------------------------
    # 3) Insertar sello al XML
    # -------------------------
    def insertar_sello_en_xml(self, xml_in, xml_out, sello_b64):
        """
        Inserta/actualiza el atributo Sello en el nodo raíz (cfdi:Comprobante).
        """
        parser = etree.XMLParser(remove_blank_text=False, recover=False)
        tree = etree.parse(xml_in, parser)
        root = tree.getroot()
        root.set("Sello", sello_b64)
        tree.write(xml_out, encoding="UTF-8", xml_declaration=True)
    def preparar_xml_para_sello(self, xml_in: str, xml_tmp: str):
        """
        Escribe un XML temporal con NoCertificado y Certificado
        (SIN Sello) para que la cadena original salga correcta.
        """
        parser = etree.XMLParser(remove_blank_text=False, recover=False)
        tree = etree.parse(xml_in, parser)
        root = tree.getroot()

        root.set("NoCertificado", self.no_certificado)
        root.set("Certificado", self.certificado_base64)

        tree.write(xml_tmp, encoding="UTF-8", xml_declaration=True)
    # -------------------------
    # 4) Flujo completo
    # -------------------------
    def sellar_xml(self, xml_in, xml_out):
        xml_tmp = xml_out.replace(".xml", ".__tmp__.xml")

        # 1) Preparar XML con NoCertificado/Certificado antes de cadena original
        self.preparar_xml_para_sello(xml_in, xml_tmp)

        # 2) Cadena original del XML ya preparado
        cadena = self.generar_cadena_original(xml_tmp)

        # 3) Generar sello con esa cadena
        sello = self.generar_sello(cadena)

        # 4) Insertar sello sobre el XML preparado (no sobre el original)
        self.insertar_sello_en_xml(xml_tmp, xml_out, sello)

        # 5) Limpieza
        try:
            os.remove(xml_tmp)
        except:
            pass

        return {"cadena_original": cadena, "sello": sello}