import psycopg2
from psycopg2 import Error # Importamos Error directamente para capturar excepciones específicas de psycopg2
import psycopg2.extras # Para usar DictCursor
import os
import traceback
from dotenv import load_dotenv
import logging

# ===============================================
# === CONFIGURACIÓN DEL LOGGER POR DEFECTO ===
# ===============================================
# Este logger se usará si no se le pasa uno al constructor de 'conexion'.
# Es una buena práctica darle un nombre específico para tu módulo de DB.
_default_db_logger = logging.getLogger("db_connector")

# Configura este logger solo si aún no tiene handlers (para evitar duplicados).
# En una aplicación real, esta configuración inicial (setLevel, addHandler)
# se haría una sola vez al inicio de tu aplicación principal o script.
if not _default_db_logger.handlers:
    _default_db_logger.setLevel(logging.ERROR) # Nivel por defecto para producción (INFO)
    handler = logging.StreamHandler() # Envía logs a la consola (stderr por defecto)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    _default_db_logger.addHandler(handler)
# ===============================================


# ===============================================
# === EXCEPCIONES PERSONALIZADAS ===
# ===============================================
class DBConnectionError(Exception):
    """Excepción para errores de conexión a la base de datos."""
    pass

class DBQueryError(Exception):
    """Excepción para errores al ejecutar consultas SQL en la base de datos."""
    pass
# ===============================================


class conexion:
    _connection = None

    def __init__(self, logger_instance=None):
        """
        Inicializa la clase de conexión a PostgreSQL.
        Carga variables de entorno y asigna una instancia de logger.

        :param logger_instance: (Opcional) Una instancia de logger de Python.
                                Si no se provee, se usa un logger por defecto.
        """
        load_dotenv() # Carga las variables del archivo .env

        self._db = os.getenv('PG_DB')
        self._user = os.getenv('PG_USER')
        self._password = os.getenv('PG_PASSWORD')
        self._host = os.getenv('PG_HOST')
        self._port = os.getenv('PG_PORT')
        # Asigna el logger: usa el que se le pasó o el logger por defecto.
        self.logger = logger_instance if logger_instance is not None else _default_db_logger
        self.logger.debug("Conexion class initialized with DB_HOST: %s, DB_PORT: %s", self._host, self._port)

    def __enter__(self):
        """
        Establece la conexión a la base de datos al entrar en un bloque 'with'.
        """
        try:
            self.logger.debug("Attempting to connect to PostgreSQL...")
            self._connection = psycopg2.connect(
                user=self._user,
                password=self._password,
                host=self._host,
                database=self._db,
                port=self._port
            )
            self._connection.autocommit = False # Asegura que las transacciones sean explícitas
            self.logger.info("Conexión a PostgreSQL establecida con éxito.")
            return self # Devuelve la instancia de Conexion para usar sus métodos
        except Error as e: # Captura el Error específico de psycopg2
            self.logger.error(f"ERROR al conectar a PostgreSQL: ")
            # Relanza como tu excepción personalizada de conexión
            raise DBConnectionError(f"No se pudo conectar a la base de datos: {e}") from e
        except Exception as e: # Captura cualquier otra excepción inesperada
            self.logger.error(f"ERROR inesperado en __enter__: ")
            raise # Re-lanza cualquier otra excepción inesperada

    def __exit__(self, exc_type, exc_value, tb):
        """
        Cierra la conexión a la base de datos al salir de un bloque 'with'.
        Maneja el rollback si hubo una excepción dentro del bloque.
        """
        if self._connection is not None:
            if exc_type:
                self.logger.error(f"Excepción ocurrida en el bloque 'with': {exc_value}\n{traceback.format_exc()}")
                self._connection.rollback()
                self.logger.warning("Transacción de base de datos revertida debido a una excepción.")

            try:
                self._connection.close()
                self.logger.debug("Conexión a PostgreSQL cerrada con éxito.")
            except Error as e:
                self.logger.error(f"Error al cerrar la conexión a PostgreSQL: {e}\n{traceback.format_exc()}")
            except Exception as e:
                self.logger.error(f"Error inesperado al cerrar la conexión: {e}\n{traceback.format_exc()}")
        else:
            self.logger.warning("Intentando cerrar una conexión nula o ya cerrada.")


    def insert(self, datos: dict, tabla: str) -> bool:
        """
        Inserta un solo registro en la tabla especificada.

        :param datos: Diccionario con los datos a insertar (claves=columnas, valores=valores).
        :param tabla: Nombre de la tabla.
        :return: True si la inserción fue exitosa.
        :raises DBQueryError: Si ocurre un error de base de datos.
        """
        try:
            cursor = self._connection.cursor()
            columnas = ', '.join([f'"{col}"' for col in datos.keys()])
            marcadores = ', '.join(['%s'] * len(datos))
            query = f"INSERT INTO {tabla} ({columnas}) VALUES ({marcadores})"
            self.logger.debug(f"Ejecutando INSERT: {query} con valores: {tuple(datos.values())}")
            cursor.execute(query, tuple(datos.values()))
            self._connection.commit()
            self.logger.info(f"Insertado exitosamente en {tabla}.")
            return True
        except Error as e:
            self._connection.rollback()
            self.logger.error(f"ERROR al insertar en {tabla}: {e}\n{traceback.format_exc()}")
            raise DBQueryError(f"Error al insertar en {tabla}: {e}") from e
        except Exception as e:
            self.logger.error(f"ERROR inesperado en insert: {e}\n{traceback.format_exc()}")
            raise
        finally:
            if 'cursor' in locals() and cursor is not None:
                try:
                    cursor.close()
                except Error as e:
                    self.logger.error(f"Error al cerrar cursor en insert: {e}")


    def insert_muchos(self, datos: list[dict], tabla: str, CONV_mayus: bool = True) -> bool:
        """
        Inserta múltiples registros en la tabla especificada en lotes.

        :param datos: Lista de diccionarios con los datos a insertar.
        :param tabla: Nombre de la tabla.
        :param CONV_mayus: Si las claves de los diccionarios deben convertirse a mayúsculas para las columnas.
        :return: True si la inserción fue exitosa.
        :raises DBQueryError: Si ocurre un error de base de datos.
        """
        if not datos:
            self.logger.warning("insert_muchos llamado con lista de datos vacía.")
            return True

        try:
            cursor = self._connection.cursor()
            if CONV_mayus:
                columnas = ', '.join([f'"{key.upper()}"' for key in datos[0].keys()])
            else:
                columnas = ', '.join(datos[0].keys())
            marcadores = ', '.join(['%s'] * len(datos[0]))
            query = f"INSERT INTO {tabla} ({columnas}) VALUES ({marcadores})"
            batch_size = 10000

            self.logger.info(f"Iniciando inserción masiva en {tabla} ({len(datos)} registros).")
            for i in range(0, len(datos), batch_size):
                batch_data = datos[i:i+batch_size]
                values = [tuple(dato.values()) for dato in batch_data]
                self.logger.debug(f"Ejecutando batch {i//batch_size + 1} de {len(batch_data)} registros.")
                cursor.executemany(query, values)

            self._connection.commit()
            self.logger.info(f"Inserción masiva completada exitosamente en {tabla}.")
            return True
        except Error as e:
            self._connection.rollback()
            self.logger.error(f"ERROR en inserción masiva en {tabla}: {e}\n{traceback.format_exc()}")
            raise DBQueryError(f"Error en inserción masiva en {tabla}: {e}") from e
        except Exception as e:
            self.logger.error(f"ERROR inesperado en insert_muchos: {e}\n{traceback.format_exc()}")
            raise
        finally:
            if 'cursor' in locals() and cursor is not None:
                try:
                    cursor.close()
                except Error as e:
                    self.logger.error(f"Error al cerrar cursor en insert_muchos: {e}")


    def query(self, datos_nesesito: str, tabla: str, where: str = None) -> list[dict]:
        """
        Ejecuta una consulta SELECT simple.
        NOTA IMPORTANTE: Este método es VULNERABLE A INYECCIÓN SQL si 'where'
        proviene directamente de entrada del usuario sin sanitizar.
        Para consultas con condiciones dinámicas seguras, use 'query_simple' con parámetros.

        :param datos_nesesito: Columnas a seleccionar (ej. "id, nombre").
        :param tabla: Nombre de la tabla.
        :param where: Cláusula WHERE sin parámetros (ej. "id = 1").
        :return: Lista de diccionarios representando las filas.
        :raises DBQueryError: Si ocurre un error de base de datos.
        """
        try:
            consulta = f"SELECT {datos_nesesito} FROM {tabla}"
            if where:
                consulta += f" WHERE {where}"
            self.logger.debug(f"Ejecutando SELECT (query): {consulta}")
            cursor = self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(consulta)
            result = cursor.fetchall()
            cursor.close()
            self.logger.info(f"Consulta a {tabla} exitosa. {len(result)} resultados.")
            return result # DictCursor ya devuelve algo iterable como diccionarios
        except Error as e:
            self.logger.error(f"ERROR al ejecutar consulta SELECT en {tabla}: {e}\n{traceback.format_exc()}")
            raise DBQueryError(f"Error al ejecutar consulta SELECT en {tabla}: {e}") from e
        except Exception as e:
            self.logger.error(f"ERROR inesperado en query: {e}\n{traceback.format_exc()}")
            raise


    def query_simple(self, consulta: str, params: tuple = None) -> list[dict]:
        """
        Ejecuta una consulta SQL simple con parámetros para prevenir inyección SQL.

        :param consulta: La cadena de consulta SQL (usar %s como marcador de posición para parámetros).
        :param params: Una tupla o lista de parámetros para la consulta.
        :return: Una lista de diccionarios (filas), o una lista vacía si no hay resultados.
                 Lanza una excepción en caso de error.
        :raises DBQueryError: Si ocurre un error de base de datos.
        """
        try:
            cursor = self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.logger.debug(f"Ejecutando query_simple: {consulta} con parámetros: {params}")
            cursor.execute(consulta, params) # ¡Aquí se pasan los parámetros de forma segura!
            result = cursor.fetchall()
            self.logger.info(f"Query_simple exitosa. {len(result)} resultados.")
            return [dict(row) for row in result] if result else [] # Convertir a dict explícitamente
        except Error as e:
            self.logger.error(f"ERROR al ejecutar query_simple en PostgreSQL: {e}\n{traceback.format_exc()}")
            raise DBQueryError(f"Error al ejecutar query_simple: {e}") from e
        except Exception as e:
            self.logger.error(f"ERROR inesperado en query_simple: {e}\n{traceback.format_exc()}")
            raise
        finally:
            if 'cursor' in locals() and cursor is not None:
                try:
                    cursor.close()
                except Error as e:
                    self.logger.error(f"Error al cerrar cursor en query_simple: {e}")


    def query_simple_sin_return(self, consulta: str, params: tuple = None) -> bool:
        """
        Ejecuta una consulta SQL sin esperar un retorno de datos (ej. DELETE, UPDATE, INSERT simple).

        :param consulta: La cadena de consulta SQL (usar %s como marcador de posición para parámetros).
        :param params: Una tupla o lista de parámetros para la consulta.
        :return: True si la operación fue exitosa.
        :raises DBQueryError: Si ocurre un error de base de datos.
        """
        try:
            cursor = self._connection.cursor()
            self.logger.debug(f"Ejecutando query_simple_sin_return: {consulta} con parámetros: {params}")
            cursor.execute(consulta, params) # Pasa parámetros también aquí
            self._connection.commit()
            self.logger.info(f"Query_simple_sin_return exitosa (sin retorno de datos).")
            return True
        except Error as e:
            self._connection.rollback()
            self.logger.error(f"ERROR al ejecutar query_simple_sin_return: {e}\n{traceback.format_exc()}")
            raise DBQueryError(f"Error al ejecutar query_simple_sin_return: {e}") from e
        except Exception as e:
            self.logger.error(f"ERROR inesperado en query_simple_sin_return: {e}\n{traceback.format_exc()}")
            raise
        finally:
            if 'cursor' in locals() and cursor is not None:
                try:
                    cursor.close()
                except Error as e:
                    self.logger.error(f"Error al cerrar cursor en query_simple_sin_return: {e}")


    def update(self, datos, tabla, where_clause, where_params) -> bool:
        """
        Actualiza registros en la tabla especificada.

        :param datos: Diccionario con las columnas y valores a actualizar.
        :param tabla: Nombre de la tabla.
        :param where_clause: Cláusula WHERE (ej. "id = %s").
        :param where_params: Tupla o lista de parámetros para la cláusula WHERE.
        :return: True si la actualización fue exitosa.
        :raises DBQueryError: Si ocurre un error de base de datos.
        """
        try:
            cursor = self._connection.cursor()
            set_clause = ', '.join([f'"{columna}" = %s' for columna in datos.keys()])
            query = f"UPDATE {tabla} SET {set_clause}"
            values = list(datos.values()) # Convertir a lista para poder extenderla

            if where_clause:
                query += f" WHERE {where_clause}"
                if where_params: # Añadir parámetros del WHERE si existen
                    values.extend(where_params)

            self.logger.debug(f"Ejecutando UPDATE: {query} con valores: {tuple(values)}")
            cursor.execute(query, tuple(values))
            self._connection.commit()
            self.logger.info(f"Actualización exitosa en {tabla}.")
            return True
        except Error as e:
            self._connection.rollback()
            self.logger.error(f"ERROR al actualizar en {tabla}: {e}\n{traceback.format_exc()}")
            raise DBQueryError(f"Error al actualizar en {tabla}: {e}") from e
        except Exception as e:
            self.logger.error(f"ERROR inesperado en update: {e}\n{traceback.format_exc()}")
            raise
        finally:
            if 'cursor' in locals() and cursor is not None:
                try:
                    cursor.close()
                except Error as e:
                    self.logger.error(f"Error al cerrar cursor en update: {e}")


    def delete(self, tabla: str, where_clause: str = None, where_params: tuple = None) -> bool:
        """
        Elimina registros de la tabla especificada.

        :param tabla: Nombre de la tabla.
        :param where_clause: Cláusula WHERE (ej. "id = %s").
        :param where_params: Tupla o lista de parámetros para la cláusula WHERE.
        :return: True si la eliminación fue exitosa.
        :raises DBQueryError: Si ocurre un error de base de datos.
        """
        try:
            cursor = self._connection.cursor()
            consulta = f"DELETE FROM {tabla}"
            values = []

            if where_clause:
                consulta += f" WHERE {where_clause}"
                if where_params:
                    values.extend(where_params)

            self.logger.debug(f"Ejecutando DELETE: {consulta} con valores: {tuple(values)}")
            cursor.execute(consulta, tuple(values))
            self._connection.commit()
            self.logger.info(f"Eliminación exitosa en {tabla}.")
            return True
        except Error as e:
            self._connection.rollback()
            self.logger.error(f"ERROR al eliminar en {tabla}: {e}\n{traceback.format_exc()}")
            raise DBQueryError(f"Error al eliminar en {tabla}: {e}") from e
        except Exception as e:
            self.logger.error(f"ERROR inesperado en delete: {e}\n{traceback.format_exc()}")
            raise
        finally:
            if 'cursor' in locals() and cursor is not None:
                try:
                    cursor.close()
                except Error as e:
                    self.logger.error(f"Error al cerrar cursor en delete: {e}")