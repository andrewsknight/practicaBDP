
# Memoria del proyecto:


## Infraesctructura
- Una instancia en **google cloud** con un contenedor docker de  **Apache Kafka**
![Instancia kafka](https://github.com/andrewsknight/practicaBDP/raw/main/images/instancia_Kafka.jpg)

- SQL desplegado en **google cloud** con **Postgress**
![Postgress](https://github.com/andrewsknight/practicaBDP/raw/main/images/sqlGcloud.jpg)
## Estructura del proyecto
![Estrucutra del proyecto](https://github.com/andrewsknight/practicaBDP/raw/main/images/estructura_del_proyecto.jpg)
- BytesStreamingJob
	- Contiene la logica para el procesamiento de **datos en streaming**,  extediende de la clase **StreamingJob**
- AntennaBatchJob
	- Contiene la logica del **proceso en batch**, extiende de la clase **BatchJob**

## Objetivo

### Procesamiento en streaming, tiempo real
Consiste en recoger datos en streaming de kafka, transformarlos con datos obtenidos de un Postgress.
El resultado se guarda en Postgress en otras tablas y también en disco en formato **Parquet** para realizar tareas en batch posteriormente.
![Tabla bytes en la que se ven las diferentes](https://github.com/andrewsknight/practicaBDP/raw/main/images/bytes_antenna_total_bytes.jpg )
-   Total de bytes recibidos por antena. `tabla bytes, tipo: [antenna_total_bytes]`
-   Total de bytes transmitidos por id de usuario.  `tabla bytes, tipo: [user_total_bytes]`
-   Total de bytes transmitidos por aplicación  `tabla bytes, tipo: [app_total_bytes]`

![Recepcion datos de kafka](https://github.com/andrewsknight/practicaBDP/raw/main/images/batchKafka.jpg)
-   Datos guardados en disco con el formato de **Parquet**, se usarán el el siguiente proceso
	- Se guardan en la carpeta **/tmp/datos/antenna_parquet/**

### Procesamiento en batch
![Estructura de datos en parquet](https://github.com/andrewsknight/practicaBDP/raw/main/images/estructura_carpetas_parquet.jpg)
Con los datos obtenidos en el proceso de tiempo real realizamos cálculos y se persisten en el postgress.

-   Total de bytes recibidos por antena.
-   Total de bytes transmitidos por mail de usuario.
-   Total de bytes transmitidos por aplicación.
-   Email de usuarios que han sobrepasado la cuota por hora.

## Autor

Andres Caballero