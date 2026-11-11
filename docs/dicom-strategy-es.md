# Estrategia DICOM para migración y explotación de metadatos

27 de octubre de 2026

Proponemos mantener en MongoDB los catálogos de estudios y series que necesita el visor, conservar referencias compactas a las instancias importadas y almacenar los archivos DICOM y las cabeceras extensas en almacenamiento de objetos. Para investigación, añadimos únicamente las proyecciones que necesita cada caso de uso.

El objetivo es reducir el coste de la información que permanece activa y ampliar las consultas sobre metadatos. El diseño está orientado a 15.000 millones de instancias. Su capacidad y coste a esa escala deben comprobarse con datos representativos y pruebas de carga; la versión actual de Kehrnel es una implementación de evaluación.

## Qué aporta respecto al modelo actual

El modelo de estudios con series embebidas y una colección separada de instancias es un punto de partida útil. Conservamos su información de origen y añadimos un catálogo plano de series, campos preparados para las consultas frecuentes y una colección opcional de investigación. La lista de instancias permanece separada de los documentos de estudios y series.

| Información | Dónde se conserva | Para qué se utiliza |
| --- | --- | --- |
| Estudios | dicom_studies en MongoDB | Paciente, fecha, modalidad y descripción del estudio |
| Series | dicom_series en MongoDB | Series de un estudio y consultas directas por serie |
| Instancias | dicom_instances en MongoDB | Listado ordenado y referencia a metadatos retenidos |
| Cabeceras extensas | Objetos BSON por lotes | Recuperación de los campos solicitados |
| Metadatos promovidos | dicom_research opcional | Filtros de investigación por atributos seleccionados |
| Archivos DICOM | Almacenamiento de objetos | Entrega de imágenes mediante la integración del visor |

## Cómo funciona la estrategia

### El recorrido de una consulta

El visor selecciona primero estudios o series por paciente, fecha, modalidad o descripción. Después solicita una página de instancias de la serie elegida. Solo al necesitar los detalles pide las cabeceras retenidas. La entrega de píxeles se resuelve mediante la integración de imágenes correspondiente.

Este recorrido evita consultar las cabeceras de miles de millones de instancias para las búsquedas habituales. Los índices de estudios y series atienden la selección inicial. El índice de instancias atiende una serie concreta.

### Ingestión continua sin un documento que crezca con cada imagen

Cada instancia se escribe de forma independiente. La llegada de cien instancias de una misma serie produce cien registros de instancia, sin añadir cien elementos a un gran array del estudio. La actualización de los recuentos se ejecuta por separado.

El catálogo de origen puede seguir incluyendo un pequeño resumen de sus series. Su actualización se realiza cuando cambia ese catálogo, no obligatoriamente con cada instancia recibida. El recuento declarado por el origen y el número de cabeceras importadas se mantienen separados. Un intervalo sin actividad no demuestra que una serie haya terminado.

### Una estrategia común para diferentes proveedores

El modelo y sus consultas son aplicables a distintos hyperscalers. Azure corresponde al despliegue elegido por el cliente, pero no condiciona la estructura DICOM. Kehrnel dispone actualmente de adaptadores de artefactos para sistema de archivos, S3 y Azure Blob. Un adaptador nativo de Google Cloud Storage y las pruebas de cada proveedor son trabajo adicional.

La decisión de despliegue debe comparar almacenamiento, índices, réplicas, peticiones a objetos, transferencias, restauración y tiempo de construcción de proyectos. Almacenar base de datos y objetos en diferentes nubes puede modificar tanto el coste como la latencia.

### Opciones según presupuesto

| Perfil | Configuración | Contrapartida |
| --- | --- | --- |
| Inicial | Catálogos disponibles y metadatos de instancia importados progresivamente | Se conserva la resolución del sistema anterior para información aún no migrada |
| Equilibrado | Catálogos y referencias compactas en MongoDB; cabeceras en objetos | La consulta de detalles necesita recuperar un objeto; requiere medir peticiones y latencia |
| Investigación ampliada | Proyecciones adicionales para los campos y poblaciones solicitados | Mayor coste de índices, construcción, mantenimiento y conservación de resultados |

## Un estudio de ejemplo en las distintas colecciones

El ejemplo utiliza identificadores y datos ficticios. Representa una resonancia de rodilla del centro DEMO_A con tres series. El origen declara 12 instancias por serie. Se han importado las cabeceras de cuatro instancias de la primera serie y dos de la segunda; la tercera conserva su catálogo, pero todavía no tiene cabeceras importadas.

| Serie | Descripción | Declaradas | Importadas |
| --- | --- | --- | --- |
| 2.25.2001 | Rodilla sagital T2 | 12 | 4 |
| 2.25.2001.1 | Rodilla coronal PD | 12 | 2 |
| 2.25.2001.2 | Rodilla axial T2 | 12 | 0 |

Un UID es el identificador DICOM del estudio, la serie o la instancia. Un SOP representa un objeto DICOM; no siempre equivale a una sola imagen o fotograma. MR es el código DICOM de resonancia magnética; CT corresponde a tomografía y MG a mamografía.

### Colección de estudios

Contiene un documento por estudio. Los campos de origen se conservan y el bloque _kehrnel añade los campos de consulta. Los ejemplos siguientes muestran los campos principales; los registros completos también incorporan huellas de contenido y referencias a las versiones de origen.

```json
{
  "_id": "2.25.1001",
  "dm": "DEMO_A",
  "pi": "PACIENTE_DEMO_001",
  "de": "Resonancia de rodilla",
  "dt": {"$date": "2026-01-01T00:00:00Z"},
  "se": [
    {"si": "2.25.2001", "ci": "DEMO_A", "mo": "MR", "ni": 12},
    {"si": "2.25.2001.1", "ci": "DEMO_A", "mo": "MR", "ni": 12},
    {"si": "2.25.2001.2", "ci": "DEMO_A", "mo": "MR", "ni": 12}
  ],
  "_kehrnel": {
    "tenantId": "demo",
    "centerId": "DEMO_A",
    "studyUid": "2.25.1001",
    "patientId": "PACIENTE_DEMO_001",
    "day": "2026-01-01",
    "description": "resonancia de rodilla",
    "modalities": ["MR"],
    "seriesCount": 3,
    "seriesFacets": [
      {"centerId": "DEMO_A", "modality": "MR", "description": "rodilla sagital t2"},
      {"centerId": "DEMO_A", "modality": "MR", "description": "rodilla coronal pd"},
      {"centerId": "DEMO_A", "modality": "MR", "description": "rodilla axial t2"}
    ]
  }
}
```

dm identifica el centro de origen y pi el identificador local del paciente. El bloque se contiene resúmenes de series, no las instancias ni los píxeles. day es una fecha preparada para filtrar. Las fechas del modelo actual se interpretan a partir de dt en UTC; una política de calendario local deberá definirse expresamente.

## La colección de series

dicom_series contiene un documento por serie. Permite consultar todas las series de un estudio o filtrar series directamente por paciente, modalidad, día y descripción. El bloque source conserva el resumen recibido del catálogo original.

```json
{
  "_id": "2.25.2001",
  "source": {
    "si": "2.25.2001",
    "ci": "DEMO_A",
    "sn": "1",
    "mo": "MR",
    "ni": 12,
    "de": "Rodilla sagital T2"
  },
  "_kehrnel": {
    "tenantId": "demo",
    "centerId": "DEMO_A",
    "studyUid": "2.25.1001",
    "seriesUid": "2.25.2001",
    "patientId": "PACIENTE_DEMO_001",
    "day": "2026-01-01",
    "modality": "MR",
    "description": "rodilla sagital t2",
    "metadataSummary": {
      "importedInstanceDocuments": 4,
      "sourceReportedCount": 12,
      "asOf": {"$date": "2026-10-27T09:52:34.600Z"},
      "metadataCoverage": "partial_or_unknown"
    }
  }
}
```

### Cómo interpretar los recuentos

sourceReportedCount vale 12 porque es el número comunicado por el origen. importedInstanceDocuments vale 4 porque se han importado cuatro registros de instancia. asOf indica cuándo se calculó el recuento. metadataCoverage permanece parcial o desconocida mientras no exista evidencia de que la importación cubre todo el conjunto esperado.

El centro de la serie es autoritativo para sus instancias. El paciente se identifica dentro de su centro; dos centros pueden utilizar el mismo identificador local para personas diferentes. La búsqueda nunca debe unirlos únicamente por pi. Una identificación compartida requiere un contrato de vinculación adicional.

El catálogo no presupone que fabricante o espesor sean iguales en todas las instancias. Esos atributos se consultan a través de proyecciones con evidencia de instancia hasta disponer de una agregación de serie validada.

## La colección de instancias

En el perfil equilibrado, dicom_instances conserva un registro compacto por cada SOP importado. La cabecera completa se mantiene en un objeto de metadatos. La referencia se publica después de escribir ese objeto y permite comprobar su integridad.

```json
{
  "_id": "2.25.100003",
  "_kehrnel": {
    "tenantId": "demo",
    "centerId": "DEMO_A",
    "studyUid": "2.25.1001",
    "seriesUid": "2.25.2001",
    "sopUid": "2.25.100003",
    "instanceNumber": 3,
    "orderMissing": 0,
    "headerRef": {
      "key": "dicom/demo/metadata/fad9ff8be4b6dbe98350233027c430370f1a689f78a2f7b97a27cec6e55a0186.bson",
      "sha256": "fad9ff8be4b6dbe98350233027c430370f1a689f78a2f7b97a27cec6e55a0186",
      "bytes": 2558,
      "ordinal": 2,
      "format": "dicom-metadata-bundle/v1"
    }
  }
}
```

instanceNumber es numérico, aunque el origen DICOM lo represente como texto. Así se obtiene el orden 1, 2, 3, 10. orderMissing permite colocar al final las instancias sin número utilizable. El UID de SOP resuelve empates; la ordenación geométrica de imágenes y fotogramas sigue siendo responsabilidad del visor.

headerRef.key es una clave estable, no una URL firmada que caduca. sha256 comprueba los bytes recuperados. ordinal indica la posición del registro dentro del lote; no es un desplazamiento en bytes. El modo actual recupera el lote completo, con un máximo configurado de 4 MiB por objeto.

Los datos compartidos que pueden cambiar, como paciente, modalidad y fecha, permanecen en los catálogos y en las proyecciones que los necesitan. El localizador no repite esos campos. Esto reduce las actualizaciones masivas cuando cambia un dato del catálogo.

### Cabecera dentro de MongoDB como alternativa

Kehrnel también ofrece el modo inline, que conserva header en el documento de instancia. Resulta útil para una prueba inicial o para casos donde el coste de recuperación de objetos no compense. El almacenamiento de cabeceras en objetos es una configuración explícita, no el valor por defecto.

## Los metadatos retenidos en almacenamiento de objetos

El archivo DICOM con sus píxeles y el objeto que conserva sus metadatos son dos elementos diferentes. Kehrnel agrupa registros originales de metadatos en lotes BSON acotados. BSON es la representación binaria de documentos utilizada por MongoDB; para facilitar la lectura, este ejemplo se muestra en JSON.

```json
{
  "_id": "2.25.100003",
  "header": {
    "00080018": {"vr": "UI", "Value": ["2.25.100003"]},
    "0020000D": {"vr": "UI", "Value": ["2.25.1001"]},
    "0020000E": {"vr": "UI", "Value": ["2.25.2001"]},
    "00200013": {"vr": "IS", "Value": ["3"]},
    "00080060": {"vr": "CS", "Value": ["MR"]},
    "00080070": {"vr": "LO", "Value": ["FABRICANTE_DEMO"]},
    "00180050": {"vr": "DS", "Value": ["1.5"]}
  }
}
```

Las claves de ocho caracteres identifican atributos DICOM. vr expresa el tipo de valor y Value conserva sus valores originales. Por ejemplo, 00080070 es el fabricante, 00180050 el espesor de corte y 00200013 el número de instancia. La conservación del original evita tener que volver a abrir el archivo DICOM cada vez que se solicitan estos metadatos ya importados.

El modo actual conserva los registros completos dentro de los lotes. No aplica todavía una separación de atributos comunes por serie ni una lectura por rango de bytes. Esas optimizaciones deben justificar su complejidad y medirse con cabeceras representativas de CT, MR y objetos multiframe.

## La colección opcional de investigación

dicom_research es una proyección: una copia pequeña de los atributos seleccionados, preparada para filtros concretos. En este ejemplo se han promovido fabricante y espesor para la modalidad MR. Se construye por un proceso separado de la ingestión ordinaria.

```json
{
  "_id": "2.25.100003",
  "_kehrnel": {
    "tenantId": "demo",
    "centerId": "DEMO_A",
    "studyUid": "2.25.1001",
    "seriesUid": "2.25.2001",
    "patientId": "PACIENTE_DEMO_001",
    "day": "2026-01-01",
    "modality": "MR"
  },
  "attrs": [{"p": "00080070", "s": "FABRICANTE_DEMO"}, {"p": "00180050", "n": 1.5}],
  "projectionVersion": "v1:c696ccd938712270b803148fbcd2a396a99bc2bd1397a2e76e727cebc64f2a10"
}
```

En attrs, p identifica el atributo, s contiene un texto y n contiene un número normalizado. projectionVersion identifica la configuración con la que se generó la proyección. Las cabeceras originales conservan su VR y valor fuente; la proyección guarda el valor preparado para consultar.

### Qué garantiza y qué no garantiza esta proyección

Un filtro que combine fabricante y espesor debe encontrar ambos en la misma instancia. No se permite combinar el fabricante de una instancia con el espesor de otra para producir una coincidencia falsa. Dentro de cada atributo, sus límites numéricos deben corresponder al mismo valor.

La consulta actual devuelve filas de instancia con referencias a estudio y serie. Cinco filas no significan cinco estudios ni cinco pacientes. La cobertura puede ser parcial y el solicitante debe aceptarlo explícitamente. Los resultados vacíos no demuestran que un atributo o una condición clínica estén ausentes en la población completa.

El futuro recorrido en Healthcare Data Lab permitirá elegir campos, ámbito y tipo de resultado, estimar la construcción, esperar su finalización y publicar una versión reproducible. La versión actual dispone de configuración y reconstrucción por lotes; el trabajo con proyectos publicados y cohortes completas está pendiente.

## Los índices que atienden las consultas habituales

El plan actual utiliza índices regulares de MongoDB. En cada fila, T significa _kehrnel.tenantId y C significa _kehrnel.centerId. Todos los campos internos abreviados de las tablas pertenecen a _kehrnel; _id es el UID del documento. Las claves son ascendentes y el prefijo T C acota organización y centro.

| Colección | Campos después de T C | Consulta atendida |
| --- | --- | --- |
| dicom_studies | patientId, day, _id | Estudios de un paciente y período |
| dicom_studies | day, _id | Estudios por fecha |
| dicom_studies | description, day, _id | Descripción exacta o prefijo acotado |
| dicom_studies | seriesFacets.modality, day, _id | Estudios que contienen la modalidad solicitada |
| dicom_series | studyUid, day, _id | Series de un estudio |
| dicom_series | modality, day, _id | Series por modalidad y fecha |
| dicom_series | patientId, day, _id | Series de un paciente |
| dicom_series | description, day, _id | Descripción de serie |
| dicom_instances | seriesUid, orderMissing, instanceNumber, _id | Página ordenada de una serie |

MongoDB crea además el índice _id_ de cada colección. La implementación de evaluación conserva los UID originales y rechaza conflictos entre ámbitos. La garantía de unicidad y el enrutamiento en una colección fragmentada deberán diseñarse junto con su clave de sharding.

### Ejemplo del índice de instancias

```json
db.dicom_instances.createIndex({
  "_kehrnel.tenantId": 1,
  "_kehrnel.centerId": 1,
  "_kehrnel.seriesUid": 1,
  "_kehrnel.orderMissing": 1,
  "_kehrnel.instanceNumber": 1,
  "_id": 1
}, {name: "idx_dicom_instance_series_order"})
```

El índice facilita una lectura acotada; no garantiza que todos los documentos de la serie estén físicamente juntos en disco. Tampoco es la clave de sharding. El despliegue debe medir planes, concentración de escrituras y series excepcionalmente grandes.

## Los índices de investigación y el papel de Search

Cuando se activa la proyección, se añaden tres índices regulares. En esta tabla, los nombres attrs y projectionVersion están en la raíz del documento.

| Índice | Campos después de T C | Uso |
| --- | --- | --- |
| idx_dicom_research_scope | projectionVersion, _kehrnel.modality, _kehrnel.day, _id | Versión, modalidad y período |
| idx_dicom_research_strings | projectionVersion, attrs.p, attrs.s | Atributo y valor de texto |
| idx_dicom_research_numbers | projectionVersion, attrs.p, attrs.n | Atributo y valor numérico |

La combinación de atributo y valor usa $elemMatch en MongoDB. Es una condición que obliga a que ambos pertenezcan al mismo elemento de attrs. Para varios atributos se comprueba cada pareja en la misma fila de instancia. Estos índices no garantizan por sí solos cualquier combinación arbitraria con latencia constante; el proyecto debe validar su plan.

### Nuestra recomendación sobre Atlas Search

Las proyecciones selectivas son una pieza importante de la estrategia. Permiten conservar el original y pagar por los campos de búsqueda que aportan valor. Atlas Search puede ser útil para búsquedas de texto más flexibles, facetas y combinaciones que lo justifiquen, sobre una colección de proyección acotada.

Para el visor actual, las descripciones exactas y los prefijos se atienden con índices regulares. El prefijo y la selección de estudios por descripción de serie están limitados a un día en la implementación de evaluación. No es necesario indexar todas las cabeceras de las 15.000 millones de instancias para proporcionar estas consultas.

En Atlas Search, embeddedDocument aporta la correlación por elemento, siempre que el array esté indexado como embeddedDocuments. Cada elemento aumenta el número de objetos indexados, por lo que la cantidad de atributos promovidos debe estar acotada.

Kehrnel puede planificar y crear una definición opcional de Search para la proyección. La ejecución de consultas DICOM mediante Atlas Search aún está pendiente. Por ello, los ejemplos de este documento utilizan MongoDB y no requieren Search.

### Proyectos históricos y almacenamiento de archivo

Una vista calculada bajo demanda no debe confundirse con una colección materializada y sincronizada. La investigación futura construirá documentos físicos para el conjunto elegido y conservará su cobertura. Un origen archivado puede alimentar esa construcción si su integración lo permite; no se asume que una vista de Archive, Federation o Atlas Infinite pueda recibir directamente un índice Search.

## Ejemplos de consultas para el visor

La aplicación utiliza el contrato dicom-query/v1. Kehrnel añade el ámbito autorizado y compila la consulta a MongoDB. El parámetro centers solo puede reducir los centros permitidos; nunca otorga acceso a un centro nuevo. Los siguientes ejemplos utilizan el estudio de demostración.

### Estudios de un paciente en una fecha

```json
{
  "version": "dicom-query/v1",
  "centers": ["DEMO_A"],
  "limit": 100,
  "resource": "studies",
  "filter": {"patientId": "PACIENTE_DEMO_001", "day": "2026-01-01"}
}
```

### Resonancias realizadas en una fecha

```json
{
  "version": "dicom-query/v1",
  "centers": ["DEMO_A"],
  "limit": 100,
  "resource": "studies",
  "filter": {"modality": "MR", "day": "2026-01-01"}
}
```

### Series de un estudio

```json
{
  "version": "dicom-query/v1",
  "centers": ["DEMO_A"],
  "limit": 100,
  "resource": "series",
  "filter": {"studyUid": "2.25.1001"}
}
```

### Instancias de una serie en orden

```json
{
  "version": "dicom-query/v1",
  "centers": ["DEMO_A"],
  "limit": 100,
  "resource": "instances",
  "filter": {"seriesUid": "2.25.2001"}
}
```

Las consultas de instancias requieren una serie concreta. El visor navega de estudio a series y solicita páginas de instancias. El máximo actual es de 500 filas por página; para continuar se utiliza el cursor devuelto por Kehrnel. El cursor no promete una fotografía inmutable de un conjunto que sigue recibiendo datos.

## Consultas por descripción período y metadatos

### Series por prefijo de descripción

```json
{
  "version": "dicom-query/v1",
  "centers": ["DEMO_A"],
  "limit": 100,
  "resource": "series",
  "filter": {"description": "Rodilla", "descriptionMode": "prefix", "day": "2026-01-01"}
}
```

### Resonancias durante un período

```json
{
  "version": "dicom-query/v1",
  "centers": ["DEMO_A"],
  "limit": 100,
  "resource": "studies",
  "filter": {"modality": "MR", "fromDay": "2026-01-01", "toDay": "2026-02-01"}
}
```

### Investigación por fabricante y espesor

```json
{
  "version": "dicom-query/v1",
  "centers": ["DEMO_A"],
  "limit": 100,
  "resource": "research",
  "allowPartial": true,
  "filter": {
    "modality": "MR",
    "fromDay": "2026-01-01",
    "toDay": "2026-02-01",
    "tags": [
      {"path": "00080070", "equals": "FABRICANTE_DEMO"},
      {"path": "00180050", "gte": 1, "lt": 2}
    ]
  }
}
```

La descripción se normaliza para comparar mayúsculas, minúsculas y espacios, conservando el texto original. El modo exact es el predeterminado. fromDay incluye su fecha inicial y toDay excluye la final: el intervalo de enero termina al comenzar el 1 de febrero.

En la consulta de investigación, gte incluye 1 mm y lt excluye 2 mm. La cabecera de ejemplo tiene 1,5 mm y satisface ese intervalo. El resultado está expresado por instancia y allowPartial acepta expresamente su cobertura incompleta. El atributo DICOM de espesor utiliza milímetros. Para futuros proyectos, habrá que declarar las unidades y conversiones de cada campo promovido.

## Recuperación de cabeceras y futuras cohortes clínicas

### Pedir solo los detalles que necesita el visor

```json
{
  "seriesUid": "2.25.2001",
  "sopUids": ["2.25.100001", "2.25.100003"],
  "tags": ["00080070", "00180050", "00200013"],
  "includeContentIndex": false
}
```

La operación dicom_get_headers acepta una serie y hasta 100 SOP, con hasta 32 atributos seleccionados. Retorna el estado disponible, no disponible o diferido de cada selección. Una cabecera no importada no se sustituye por un documento vacío. Si se alcanza el límite de respuesta, la aplicación vuelve a solicitar los SOP diferidos con un lote menor.

La respuesta conserva el atributo DICOM completo, incluido su VR. El límite predeterminado es de 4 MiB medidos como BSON; la representación JSON transmitida puede ocupar más. ContentIndex es opcional. Estos detalles describen metadatos y no constituyen una interfaz de entrega de píxeles.

### Encontrar estudios relacionados con una condición

Fabricante, modalidad, descripción o espesor no demuestran una enfermedad. Para consultas como “estudios de pacientes con cáncer de mama” se necesita evidencia clínica vinculada: condiciones de FHIR u openEHR, información de órdenes o informes, o hallazgos codificados que realmente existan en objetos estructurados DICOM.

La ampliación deberá diferenciar diagnóstico, indicación y hallazgo, conservar su origen y fecha, definir la relación con el estudio y controlar la identificación del paciente entre centros. Si se utiliza SNOMED CT, habrá que fijar la edición y versión de terminología y resolver conjuntos grandes de conceptos sin truncarlos silenciosamente.

| Estándar o enfoque | Qué aporta al diseño |
| --- | --- |
| DICOM | Identidad, jerarquía, tipos de atributo y referencias a objetos de imagen |
| FHIR y openEHR | Intercambio y evidencia clínica vinculada a estudios y personas |
| SNOMED CT | Conceptos clínicos y relaciones terminológicas para cohortes |
| Aprendizajes de Kehrnel y CDISC | Original conservado, proyección selectiva, contratos y resultados reproducibles |

La consulta por condición no está habilitada en la versión actual. Se incorporará cuando existan fuentes clínicas y contratos de vinculación comprobables.

## Migración costes y siguientes capacidades

### Migración por etapas

Primero se precargan estudios y series desde SQL Server y se reconcilian sus cambios. La ventana nocturna debe resolver el último diferencial y el cambio de aplicación; no debe darse por hecho que 140 millones de estudios se migrarán íntegramente durante esa noche.

Las cabeceras de instancia y las copias binarias pueden incorporarse progresivamente. Mientras tanto, la aplicación conserva un resolutor que conoce la ubicación vigente de cada objeto. La ubicación solo debe actualizarse después de verificar su copia. El visor mantiene la navegación del catálogo y explica qué detalles todavía no están disponibles.

El paso a producción necesita captura duradera de cambios, reintentos, deduplicación y conciliación de correcciones y eliminaciones. Los recuentos se actualizarán mediante trabajos agrupados por serie. Esos trabajadores duraderos y la integración de migración completa son parte del plan de implementación.

### Qué significan 15 mil millones de instancias

Con 12 KB de metadatos por instancia, 15.000 millones de registros suponen unos 180 TB lógicos. Con un localizador de 1 KB, serían unos 15 TB. Son cálculos orientativos en unidades decimales, antes de índices, réplicas, compresión y objetos. El tamaño real depende de los atributos, marcos y referencias que se retengan.

Una carga de ocho millones de instancias diarias equivale a unas 93 por segundo de media, pero deben medirse sus picos. Completar 15.000 millones de instancias en siete días exigiría unas 24.800 por segundo antes de extracción, reintentos e índices. La migración debe disponer de estimaciones y capacidad de reanudación.

| Capacidad | Estado de Kehrnel |
| --- | --- |
| Catálogos, consultas, instancias independientes y cabeceras inline o en lotes | Implementación de evaluación con pruebas funcionales |
| Proyección selectiva y predicados sobre la misma instancia | Disponible con cobertura parcial |
| Perfil de origen, plantilla y arquitectura de referencia | Disponible para el recorrido de evaluación |
| Builder canónico y pantallas DICOM en Healthcare Data Lab | Siguiente integración |
| Proyectos publicados por estudio, serie o paciente y exportaciones reproducibles | Planificado |
| Migración duradera, caché histórica y fragmentación a gran escala | Pendiente de implementación y pruebas representativas |
| Cohortes clínicas y visualización de píxeles | Integraciones adicionales |

La validación funcional existente incluye MongoDB local y los contratos SDK, HTTP y CLI. No acredita todavía el rendimiento de 15.000 millones de instancias ni el coste de transferencia de un hyperscaler. La aprobación del despliegue se basará en un piloto con consultas reales, distribución representativa de series y medidas de coste y latencia.
