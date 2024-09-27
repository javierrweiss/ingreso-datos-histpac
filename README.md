# SCRIPT PARA INGRESAR HISTORIAS CLINICAS EN EL SISTEMA DEL SANATORIO DESDE CSV

Este es un programa para resolver un problema provisiorio, a saber, el ingreso de las historias clínicas de los pacientes de guardia atendidos por teleconsulta a través del sistema de reservas, en vez del de guardia que es el que le corresponde. 

Recibe un csv con las atenciones, lo parsea y guarda los datos en las tablas correspondientes (esto es, guardar la historia clínica en nuestro sistema).

La tabla principal es **tbc_histpac**, mientras que los detalles de la evaluación médica se almacenan en **tbc_histpac_txt**. Ambas son en realidad archivos COBOL, expuestos como tablas SQL a través de Relativity. 

El campo *txt4* de la tabla **tbc_histpac_txt** no admite más de 77 caracteres, por lo que hay que particionar el string y llevar la cuenta de la cantidad de líneas y la numeración que le corresponde a cada registro.

### Ejecutar el programa

```bash
clojure -X:ingresar :perfil (:prod, :dev ó :test) :ruta (e.g. C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv) :print boolean
```

### Correr contenedor para tests (WSL)

```bash
docker run -d --rm -P -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD="123456" -e POSTGRES_USER=asistencial -v /home/jrivero/postgres-data/:/var/lib/postgresql/data postgres:latest
```