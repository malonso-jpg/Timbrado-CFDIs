#!/bin/bash
DIR="/home/malonso/Documentos/Proyectos/TIMBRADO_CFDI/tmp/xslt_sat"
mkdir -p $DIR

declare -a URLS=(
  "http://www.sat.gob.mx/sitio_internet/cfd/donat/donat11.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/divisas/divisas.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/implocal/implocal.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/leyendasFiscales/leyendasFisc.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/pfic/pfic.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/TuristaPasajeroExtranjero/TuristaPasajeroExtranjero.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/nomina/nomina12.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/cfdiregistrofiscal/cfdiregistrofiscal.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/pagoenespecie/pagoenespecie.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/aerolineas/aerolineas.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/valesdedespensa/valesdedespensa.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/notariospublicos/notariospublicos.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/vehiculousado/vehiculousado.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/servicioparcialconstruccion/servicioparcialconstruccion.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/renovacionysustitucionvehiculos/renovacionysustitucionvehiculos.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/certificadodestruccion/certificadodedestruccion.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/arteantiguedades/obrasarteantiguedades.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/ComercioExterior11/ComercioExterior11.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/ComercioExterior20/ComercioExterior20.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/ine/ine11.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/iedu/iedu.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/ventavehiculos/ventavehiculos11.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/detallista/detallista.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/EstadoDeCuentaCombustible/ecc12.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/consumodecombustibles/consumodeCombustibles11.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/GastosHidrocarburos10/GastosHidrocarburos10.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/IngresosHidrocarburos10/IngresosHidrocarburos.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/CartaPorte/CartaPorte20.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/Pagos/Pagos20.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/CartaPorte/CartaPorte30.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/CartaPorte/CartaPorte31.xslt"
  "http://www.sat.gob.mx/sitio_internet/cfd/2/cadenaoriginal_2_0/utilerias.xslt"
)

for URL in "${URLS[@]}"; do
  FILENAME=$(basename $URL)
  echo "Descargando: $FILENAME"
  wget -q --timeout=10 -O "$DIR/$FILENAME" "$URL" || echo "  ⚠ Falló: $URL"
done

echo "✓ Listo. Archivos en $DIR"