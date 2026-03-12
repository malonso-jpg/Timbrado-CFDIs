#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime
import getpass
import os
from data.funcionesPostgres import conexion
from data.PACTimbrador import PACTimbrador  # ajusta el import si tu ruta cambia


# --- Config de PAC ---
WSDL_TEST = "https://pac-test.stofactura.com/pac-cancelacion-ws/cancelaCfdi33?wsdl"
PAC_SUC = "CAD221214422"
PAC_USR = "TESTUSERSTO"
PAC_PWD = "TESTPASSSTO"
pfx_path = "/home/malonso/Descargas/CSD_CAD221214422_20230726140448-20240723T201117Z-001/CSD_CAD221214422_20230726140448/certificado_legacy.pfx"
password_pfx = "21Oremoh"
def main():
    print("\n=== CANCELAR CFDI (STO PAC) ===\n")

    uuid = input("UUID a cancelar: ").strip()
    if not uuid:
        print("UUID vacío. Cancelando.")
        return

    with conexion() as db:
        pac = PACTimbrador(WSDL_TEST, PAC_SUC, PAC_USR, PAC_PWD, db)

        # 1) Previsualización (qué se va a cancelar)
        # Nota: tu query actual no es parametrizada; si tienes una versión parametrizada, úsala.
        rows = db.query("nombre_archivo, ruta_xml_timbrado, ruta_xml, timbrado",
                        "nomina.procesos_cfdi",
                        f"uuid = '{uuid}'")

        if not rows:
            print("\nNo encontré ese UUID en BD.")
            return

        row = rows[0]
        nombre_archivo = row.get("nombre_archivo")
        ruta_xml_timbrado = row.get("ruta_xml_timbrado")
        ruta_xml = row.get("ruta_xml")
        timbrado = row.get("timbrado")
        base_output_dir = os.path.dirname(ruta_xml_timbrado).replace("CFDIs_TIMBRADOS", "CFDIs_CANCELADOS")
        print("\n--- Encontrado en BD ---")
        print("Nombre archivo :", nombre_archivo)
        print("Timbrado       :", timbrado)
        print("ruta_xml_timbrado:", ruta_xml_timbrado)
        print("ruta_xml (fallback):", ruta_xml)

        # 2) Confirmación fuerte

        # 3) Ejecutar cancelación
        resp = pac.cancelar_por_uuid(uuid, str(pfx_path), password_pfx, base_output_dir=base_output_dir)

        print("\n--- RESPUESTA PAC ---")
        print("OK     :", resp.get("ok"))
        print("CODIGO :", resp.get("codigo"))
        print("UUID   :", resp.get("uuid"))
        print("MENSAJE:", resp.get("mensaje") or resp.get("error"))
        print("ACUSE  :", resp.get("ruta_acuse"))

    print("\nListo.\n")


if __name__ == "__main__":
    main()