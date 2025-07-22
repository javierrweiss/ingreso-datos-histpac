(ns core
  (:require
   [clojure.java.io :as io]
   [aero.core :refer [read-config]]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [honey.sql :as sql]
   [tablecloth.api :as tc]
   [babashka.fs :as fs]
   [clojure.string :as string]
   [hyperfiddle.rcf :refer [tests]]
   [com.brunobonacci.mulog :as µ]
   [portal.api :as p])
  (:import (java.sql SQLException
                     SQLIntegrityConstraintViolationException
                     SQLTimeoutException
                     SQLClientInfoException)
           (java.time LocalDateTime
                      LocalDate)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; CONFIGURACION ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def conf (read-config (io/resource "config.edn")))

(defn iniciar-mulog
  [entorno]
  (µ/set-global-context! {:app "ingreso-historias-clinicas-teleconsulta"
                          :fecha-ingreso (LocalDateTime/now)
                          :jvm-version (System/getProperty "java.version")
                          :env entorno
                          :os (System/getProperty "os.name")})
  (µ/start-publisher! {:type :simple-file
                       :filename "ingreso_datos_histpac_log/events.log"}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; UTILS ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def descripcion-diagnostico (atom nil))

(defn descomponer-hora
  "Recibe un entero representando la hora y devuelve un vector de tres elementos con la hora descompuesta en hh:mm:ss"
  [h]
  (let [hstr (str h)
        len (count hstr)]
    (case len
      4 [(->> hstr (take 2) (apply str) Integer/parseInt) (->> hstr (drop 2) (apply str) Integer/parseInt) 0]
      3 [(->> hstr (take 1) (apply str) Integer/parseInt) (->> hstr (drop 1) (apply str) Integer/parseInt) 0]
      (1 2) [0 h 0]
      nil)))

(defn sumar-minutos
  "Recibe como enteros la hora, los minutos y los minutos a sumar y devuelve un vector con dos enteros equivalente a la nueva hora y sus minutos"
  [hora minutos minutos+]
  (let [suma (+ minutos minutos+)
        diferencia (- suma 60)]
    (if (or (pos? diferencia) (== 0 diferencia))
      [(if (== 23 hora) 0 (inc hora)) diferencia]
      [hora suma])))

(defn sanitizar-string
  [st]
  (->> (seq st)
       (map #(condp some [%]
               #{\Á \À \Ã \Ä \Å} :>> (constantly \A)
               #{\á \à \â \ä \å} :>> (constantly \a)
               #{\Ê \Ë \È \É} :>> (constantly \E)
               #{\è \é \â \ë} :>> (constantly \e)
               #{\Í \Ì \Î \Ï} :>> (constantly \I)
               #{\ì \í \ï \î} :>> (constantly \i)
               #{\Ô \Ò \Õ \Ó \Ö} :>> (constantly \O)
               #{\ó \ò \ô \ö \õ} :>> (constantly \o)
               #{\Ù \Ú \Ü \Û} :>> (constantly \U)
               #{\ú \ù \ü \û} :>> (constantly \u)
               #{\Ç} :>> (constantly \C)
               #{\Ñ} :>> (constantly \N)
               #{\ñ} :>> (constantly \n)
               #{\̃  \́  \` \° \¨ \~} :>> (constantly "")
               identity :>> identity))
       (apply str)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;  SQL   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn sql-inserta-en-tbc-histpac
  [values]
  (sql/format {:insert-into :tbc_histpac
               :columns [:histpacnro ;; hc
                         :histpacfec ;; fecha ingreso tbc_guardia
                         :histpach ;; hora
                         :histpacm ;; minutos
                         :histpacr ;; resto (segundos...) completar con ceros
                         :histpace ;; 2, guardia
                         :histpacespfir ;; 407
                         :histpacnro1 ;; hc
                         :histpacfec1 ;; fecha ingreso tbc_guardia
                         :histpacnro2 ;; hc
                         :histpacespfir1 ;; 407
                         :histpacmedfir ;; 999880 (con dígito verificador) 
                         :histpacmotivo ;; numerador motivo
                         :histpacestudi ;; 0 
                         :histpachorasobre ;; 0
                         :histpachfinal ;; hora final atención
                         :histpacmfinal ;; minutos hora final atención
                         :histpacrfinal ;; completar con ceros
                         :histpacdiagno ;; diagnóstico => sacar de tbc_patologia
                         :histpacpatolo ;; 3264
                         :histpactratam ;; numerador diagnostico
                         :histpacmedfirnya ;; nombre médico 
                         :histpacmedfirmat ;; matricula 
                         :histpachatenc ;; call_start_datetime
                         :histpacmatenc  ;; minutos
                         :histpacratenc ;; 00
                         :histpacderiva ;; 0
                         :histpacderivads ;; 0
                         :histpacderivasec ;; ""
                         :histpacobra ;; obra
                         :histpacpplan ;; plan (se saca de hist_cab_new)
                         :histpacplan ;;un caracter
                         :histpacafil ;; nro afiliado
                         :histpacpedambula ;; 0
                         :histpacconshiv ;; ""
                         :histpacinterconsu ;; 0
                         :histpacentregado ;; 0
                         :histpacctro ;; 0
                         :histpacyodo ;; "N"
                         :histpaccancd ;; 0
                         ]
               :values [values]}))

(defn sql-busca-registro-en-tbc-histpac
  [histpacnro histpacfec histpach histpacm]
  (sql/format {:select :histpacnro
               :from :tbc_histpac
               :where [:and [:= :histpacnro histpacnro]
                       [:= :histpacfec histpacfec]
                       [:= :histpach histpach]
                       [:= :histpacm histpacm]
                       [:= :histpacr 0]
                       [:= :histpace 2]]}))

(defn sql-inserta-en-tbc-histpac-txt
  [values]
  (sql/format {:insert-into :tbc_histpac_txt
               :columns [:txt1
                         :txt1g
                         :txt2
                         :txt3
                         :txt4
                         :txt6]
               :values [values]}))

(defn sql-inserta-en-tbl-hist-txt
  [values]
  (sql/format {:insert-into :tbl-hist-txt
               :columns [:ht_histclin
                         :ht_fecha
                         :ht_hora
                         :ht_entrada
                         :ht_motivo
                         :ht_tratamiento]
               :values [values]}))


(defn obtener-ds-reservas
  [asistencial min-date]
  (µ/log ::obteniendo-dataset-reservas)
  (-> (tc/dataset
       (try
         (jdbc/execute! asistencial (sql/format {:select [[:tbc_reservas/reservashiscli :historiaclinica]
                                                          [:tbc_reservas/reservasfech :da]
                                                          [:tbc_hist_cab_new/histcabobra :obra]
                                                          [:tbc_hist_cab_new/histcabnrodoc :dnidelpaciente]
                                                          [:tbc_hist_cab_new/histcabplanx :plan]
                                                          [:tbc_hist_cab_new/histcabnrobenef :nro-afiliado]]
                                                 :from :tbc_reservas
                                                 :inner-join [[:tbc_hist_cab_new] [:= :tbc_reservas/reservashiscli :tbc_hist_cab_new/histcabnrounico]]
                                                 :where [:> :reservasfech min-date]}))
         (catch SQLException e (throw (let [msj (ex-message e)]
                                        (µ/log ::error-al-consultar-reservas :mensaje msj)
                                        (prn msj)
                                        (ex-info "Error al obtener dataset en reservas" {:mensaje msj}
                                                 (ex-cause e))))))
       {:key-fn #(-> % name keyword)
        :parser-fn {:historiaclinica :int32
                    :dnidelpaciente :int32}})
      (tc/unique-by [:historiaclinica])))

(defn- obtener-numerador-sql
  []
  (sql/format {:select :contador_entero
               :from :tbl_parametros
               :where [:= :paramid 16]}))

(defn- actualiza-numerador-sql
  [numerador_actual]
  (sql/format {:update :tbl_parametros
               :set  {:contador_entero (inc numerador_actual)}
               :where [:= :paramid 16]
               :returning [:contador_entero]}))

(defn obtiene-numerador!
  [conexion]
  (try
    #_(prn "Obteniendo numeradores...")
    (let [numerador-actual (-> (jdbc/execute! conexion (obtener-numerador-sql) {:builder-fn rs/as-unqualified-kebab-maps})
                               first
                               :contador-entero)
          nuevo-numerador (-> (jdbc/execute! conexion (actualiza-numerador-sql numerador-actual) {:builder-fn rs/as-unqualified-kebab-maps})
                              first
                              :contador-entero)]
      nuevo-numerador)
    (catch SQLException e (throw (let [msj (ex-message e)]
                                   (µ/log ::error-obtencion-numerador :mensaje msj)
                                   (ex-info "Error al obtener numerador: " {:conexion conexion
                                                                            :mensaje msj}
                                            (ex-cause e)))))))

(defn obtener-descripcion-diagnostico
  [conn]
  (if-let  [diag @descripcion-diagnostico]
    diag
    (let [descripcion (try
                        (-> (jdbc/execute! conn ["SELECT pat_descrip FROM tbc_patologia WHERE pat_codi = ?" 3264] {:builder-fn rs/as-unqualified-kebab-maps})
                            first
                            :pat-descrip
                            string/trim)
                        (catch SQLException e (let [msj (ex-message e)]
                                                (µ/log ::error-obtencion-descripcion-diagnostico :mensaje msj)
                                                (throw (ex-info "Error al obtener descripción de diagnóstico" {:mensaje msj} (ex-cause e)))))
                        (catch Exception e (let [msj (ex-message e)]
                                             (µ/log ::error-obtencion-descripcion-diagnostico :mensaje msj)
                                             (throw (ex-info "Error al obtener descripción de diagnóstico" {:mensaje msj} (ex-cause e))))))]
      (prn "Obteniendo descripción diagnostico...")
      (µ/log ::obtencion-descripcion-diagnostico)
      (reset! descripcion-diagnostico descripcion)
      @descripcion-diagnostico)))

(defn crear-doc-no-encontrados
  [ds]
  (try
    (tc/write! ds (str (or (System/getenv "REPORTE-NO-ENCONTRADOS") (System/getenv "HOME")) "/no-encontrados-" (LocalDate/now) ".csv"))
    (catch Exception e (let [msj (ex-message e)]
                         (µ/log ::error-crear-doc-no-encontrados :mensaje msj)
                         (throw (ex-info "Error al crear csv con historias no encontradas" {:mensaje msj} (ex-cause e)))))))
 
(defn adjuntar-info-complementaria
  [ds conexion]
  (prn "Buscando historias clínicas...")
  (let [valid-date? (fn [d] (== 8 (-> d str count)))
        min-date (->> (tc/select-rows ds (comp valid-date? :da))
                      (tc/->array :da)
                      (apply min))
        reservas  (obtener-ds-reservas conexion min-date)
        no-encontrados (-> (tc/left-join ds reservas [:da :dnidelpaciente])
                           (tc/drop-rows (fn [row] (int? (:historiaclinica row))))
                           (tc/select-columns [:da :horadeatencin :dnidelpaciente :nombredelpaciente]))]
    (when-not (== (tc/row-count no-encontrados) 0)
      (prn "Pacientes no encontrados...")
      (µ/log ::imprimiendo-pacientes-no-encontrados)
      (tc/print-dataset (tc/tail no-encontrados 50)) 
      (crear-doc-no-encontrados no-encontrados))
    (tc/inner-join reservas ds [:dnidelpaciente]))) 

(defn guarda-texto-de-historia
  [conn numerador texto & [texto2]]
  #_(prn (str "Guardando en tbc_histpac_txt \n" "Numerador: " numerador " Texto: " texto))
  (let [len (count texto)
        textos (cond
                 texto2 (if (> len 77)
                          (conj (->> (partition-all 77 texto)
                                     (mapv #(apply str %)))
                                texto2)
                          [texto texto2])
                 :else (if (> len 77)
                         (->> (partition-all 77 texto)
                              (map #(apply str %)))
                         [texto]))
        cantidad (count textos)
        contador (atom 1)]
    (try
      (jdbc/execute! conn (sql-inserta-en-tbc-histpac-txt [numerador 1 0 0 "" cantidad]) {:builder-fn rs/as-unqualified-kebab-maps})
      (doseq [text textos]
        (jdbc/execute! conn (sql-inserta-en-tbc-histpac-txt [numerador 1 @contador @contador text 0]) {:builder-fn rs/as-unqualified-kebab-maps})
        (swap! contador inc))
      ;; No propagar excepción
      (catch SQLException e #_(throw) (let [msj (ex-message e)]
                                        (µ/log ::error-guardado-texto-historia :mensaje msj)
                                        (ex-info "Error al guardar texto de historia: " {:conexion conn
                                                                                         :numerador numerador
                                                                                         :texto texto
                                                                                         :mensaje msj}
                                                 (ex-cause e)))))))

(defn armar-registros-histpac
  "Devuelve los registros en un vector de vectores de la forma [[`histpac`] [[`hispac-txt1`][`hispac-txt2`]]]"
  [{:keys [historiaclinica
           da ;; dia
           horadeatencin
           obra
           plan
           nro-afiliado
           nombredeldoctor
           mn
           diagnostico
           seguimiento
           motivo
           tratamiento]}
   conexion-desal
   conexion-maestros]
  (let [[hora minutos segundos] (descomponer-hora horadeatencin)
        [hora-fin minutos-fin] (sumar-minutos hora minutos 20)
        numerador-tratamiento (obtiene-numerador! conexion-desal)
        numerador-diagnostico (obtiene-numerador! conexion-desal)
        doctor (->> nombredeldoctor (take 29) (apply str))
        desc-diagnostico (->> (obtener-descripcion-diagnostico conexion-maestros) (take 27) (apply str))]
    [[historiaclinica
      da
      hora
      minutos
      segundos
      2
      407 ;; Sacar tbc_reservas reservas_esp
      historiaclinica
      da
      historiaclinica
      407
      999880
      numerador-tratamiento
      0
      0
      hora-fin
      minutos-fin
      0
      desc-diagnostico
      3264
      numerador-diagnostico
      doctor
      mn
      hora
      minutos
      segundos
      0
      0
      ""
      (or obra 0)
      (or plan "")
      ""
      (or nro-afiliado "")
      0
      ""
      0
      0
      0
      "N"
      0]
     [[numerador-tratamiento (str "Motivo: " motivo "\\n Seguimiento: " seguimiento)]
      [numerador-diagnostico (str "Diagnóstico: " diagnostico " Tratamiento: " tratamiento) (str "Profesional: " doctor  " Matricula: " mn)]]]))

(defn armar-registros-histpac-tbl-hist-txt
  "Devuelve los registros en un vector de vectores de la forma [[`histpac`] [`tbl-hist-txt`]]"
  [{:keys [historiaclinica
           da ;; dia
           horadeatencin
           obra
           plan
           nro-afiliado
           nombredeldoctor
           mn
           diagnostico
           seguimiento
           motivo
           tratamiento]}
   conexion-desal
   conexion-maestros]
  (let [[hora minutos segundos] (descomponer-hora horadeatencin)
        [hora-fin minutos-fin] (sumar-minutos hora minutos 20)
        numerador-tratamiento (obtiene-numerador! conexion-desal)
        numerador-diagnostico (obtiene-numerador! conexion-desal)
        doctor (->> nombredeldoctor (take 29) (apply str))
        desc-diagnostico (->> (obtener-descripcion-diagnostico conexion-maestros) (take 27) (apply str))]
    [[historiaclinica
      da
      hora
      minutos
      segundos
      2
      407 ;; Sacar tbc_reservas reservas_esp
      historiaclinica
      da
      historiaclinica
      407
      999880
      numerador-tratamiento
      0
      0
      hora-fin
      minutos-fin
      0
      desc-diagnostico
      3264
      numerador-diagnostico
      doctor
      mn
      hora
      minutos
      segundos
      0
      0
      ""
      (or obra 0)
      (or plan "")
      ""
      (or nro-afiliado "")
      0
      ""
      0
      0
      0
      "N"
      0]
     [historiaclinica
      da
      hora
      1
      (str "Motivo " "Nro. " numerador-tratamiento ": " motivo)
      (str "Diagnóstico: " diagnostico "\nTratamiento " "Nro. " numerador-diagnostico ": " historiaclinica "\nMédico: " doctor "\nMatrícula: " mn)]]))

(defn insertar-en-tbc-histpac
  [registros conexion]
  (prn "Insertando registros en tbc_histpac...")
  (µ/log ::insercion-tbc-histpac)
  (when (== 0 (count registros))
    (µ/log ::no-existen-registros-para-insertar)
    (throw (ex-info "No existen registros para insertar" {:registros registros})))
  (let [cantidad (atom 0)]
    (doseq [registro registros] (try
                                  (-> (jdbc/execute! conexion (sql-inserta-en-tbc-histpac registro) {:builder-fn rs/as-unqualified-kebab-maps})
                                      first
                                      :next.jdbc/update-count)
                                  (swap! cantidad inc)
                                  (catch SQLException e (let [msj (ex-message e)]
                                                          (prn (str "Hubo un problema al insertar en tbc_histpac " msj))
                                                          (µ/log ::error-insercion-tbc-histpac :mensaje msj :registro registro)
                                                          #_(throw)
                                                          ;; No propagar excepción
                                                          (ex-info "Error al insertar en tbc_histpac" {:mensaje msj :registro registro} (ex-cause e))))))
    (prn (str @cantidad " registro(s) insertado(s)"))
    (µ/log ::registros-insertados :cantidad @cantidad)))

(defn insertar-en-tbl-hist-txt
  [registros desal-con]
  (prn "Insertando registros en tbl_hist_txt...")
  (µ/log ::insercion-tbl-hist-txt)
  (doseq [registro registros] (try
                                (jdbc/execute! desal-con (sql-inserta-en-tbl-hist-txt registro) {:builder-fn rs/as-unqualified-kebab-maps}) 
                                (catch SQLException e (let [msj (ex-message e)]
                                                        (prn (str "Hubo un problema al insertar en tbl_hist_txt " msj))
                                                        (µ/log ::error-insercion-tbl-hist-txt :mensaje msj :registro registro) 
                                                        (ex-info "Error al insertar en tbl_hist_txt" {:mensaje msj :registro registro} (ex-cause e)))))))

(defn existe-registro?
  [conn-asistencial hc fecha hora minutos]
  (let [busqueda (try
                   (-> (jdbc/execute! conn-asistencial (sql-busca-registro-en-tbc-histpac hc fecha hora minutos) {:builder-fn rs/as-unqualified-kebab-maps})
                       first)
                   (catch SQLException e (let [msj (ex-message e)]
                                           (µ/log ::error-busqueda-confirmacion-registro-existente :mensaje msj)
                                           (throw (ex-info "Error al buscar registro en tbc_histpac" {:mensaje msj} (ex-cause e)))))
                   (catch Exception e (let [msj (ex-message e)]
                                        (µ/log ::error-busqueda-confirmacion-registro-existente :mensaje msj)
                                        (throw (ex-info "Error al buscar registro en tbc_histpac" {:mensaje msj} (ex-cause e))))))]
    (when busqueda
      true)))

(defn filtrar-registros-existentes
  "Recibe un vector de vectores como el producido por `armar-registros-histpac` o `armar-registros-histpac-tbl-hist-txt` y devuelve un vector con las mismas propiedades con excepción de 
   los vectores que contienen la información de los registros ya insertados"
  [registros-vec conexion-asistencial]
  (prn "Buscando registros ya guardados...")
  (µ/log ::busqueda-registros-ya-existentes)
  (let [registros (into [] (remove (fn [[histpac]]
                                     (apply existe-registro? conexion-asistencial (take 4 histpac))))
                        registros-vec)]
    (prn "Nro de registros para guardar: " (count registros))
    (µ/log ::nro-registros-para-guardar :cantidad (count registros))
    registros))

(defn inserta-en-tablas-histpac
  [ds-vec desal-conn maestros-conn asistencial-conn]
  (prn "Comenzando la inserción de registros...")
  (µ/log ::inicio-insercion-tablas-histpac)
  (try
    (let [registros (mapv #(armar-registros-histpac % desal-conn maestros-conn) ds-vec)
          registros-filtrados (filtrar-registros-existentes registros asistencial-conn)
          registros-histpac (mapv first registros-filtrados)
          registros-histpac-txt (mapv second registros-filtrados)]
      (insertar-en-tbc-histpac registros-histpac asistencial-conn)
      (mapv (fn [[registro-tratamiento registro-diagnostico]]
              (apply guarda-texto-de-historia asistencial-conn registro-tratamiento)
              (apply guarda-texto-de-historia asistencial-conn registro-diagnostico))
            registros-histpac-txt))
    (prn "¡Registros insertados con éxito en tbc_histpac y tbc_histpac_txt!")
    (µ/log ::insercion-exitosa-tbc-histpac-en-y-tbc-histpac-txt)
    (catch SQLException e (let [msj (ex-message e)]
                            (µ/log ::error-insercion-tablas-histpac :mensaje msj)
                            (throw (ex-info "Hubo una excepción al insertar en tbc_histpac / tbc_histpac_txt: " {:mensaje msj} (ex-cause e)))))))

(defn inserta-en-tablas-histpac-new
  [ds-vec desal-conn maestros-conn asistencial-conn]
  (prn "Comenzando la inserción de registros => tbc_histpac y textos en tbl_hist_txt...")
  (µ/log ::inicio-insercion-tablas-histpac-textos-en-tbl-hist-txt)
  (try
    (let [registros (mapv #(armar-registros-histpac-tbl-hist-txt % desal-conn maestros-conn) ds-vec)
          registros-filtrados (filtrar-registros-existentes registros asistencial-conn)
          registros-histpac (mapv first registros-filtrados)
          registros-hist-txt (mapv second registros-filtrados)]
      (insertar-en-tbc-histpac registros-histpac asistencial-conn)
      (insertar-en-tbl-hist-txt registros-hist-txt desal-conn))
    (catch Exception e (let [msj (ex-message e)]
                         (µ/log ::error-insercion-tablas-histpac-tbl-hist-txt :mensaje msj)
                         (throw (ex-info "Hubo una excepción al insertar en tbc_histpac / tbl_hist_txt: " {:mensaje msj} (ex-cause e)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; PROCESA ARCHIVO CSV ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn leer-csv
  ([ruta]
   (leer-csv ruta false))
  ([ruta print?]
   (prn (str "Archivo a procesar: " ruta))
   (letfn [(str->keywd [s]
             (-> s
                 (string/replace #"\W" "")
                 string/lower-case
                 keyword))]
     (cond
       (not (fs/exists? (io/file ruta))) (prn "El archivo seleccionado no existe")
       print? (prn "Imprimiendo resultados: \n" (-> (tc/dataset ruta {:key-fn str->keywd})
                                                    (tc/select-columns [:da :horadeatencin :nombredeldoctor :mn :nombredelpaciente])))
       :else (tc/dataset ruta {:key-fn str->keywd
                               :parser-fn {"Día" :string
                                           "Fecha de Nacimiento" :string}})))))

(defn normalizar-datos
  [ds]
  (prn "Parseando datos...")
  (µ/log ::inicio-parseo-csv)
  (when (tc/dataset? ds)
    (try
      (-> ds
          (tc/drop-missing [:da :horadeatencin])
          (tc/map-columns :da #(when %
                                 (as-> % f
                                   (string/split f #"/|-")
                                   (reverse f)
                                   (apply str f)
                                   (Integer/parseInt f))))
          (tc/map-columns :fechadenacimiento #(when %
                                                (as-> % f
                                                  (string/split f #"/")
                                                  (reverse f)
                                                  (apply str f)
                                                  (Integer/parseInt f))))
          (tc/map-columns :horadeatencin #(-> %
                                              (string/replace #"\s|:|hs" "")
                                              Integer/parseInt))
          (tc/map-columns :nombredeldoctor #(-> % string/upper-case sanitizar-string))
          (tc/map-columns :nombredelpaciente #(-> % string/upper-case sanitizar-string))
          (tc/map-columns :seguimientoevolucin (fnil sanitizar-string ""))
          (tc/map-columns :diagnstico (fnil sanitizar-string ""))
          (tc/map-columns :motivo (fnil sanitizar-string ""))
          (tc/map-columns :tratamiento (fnil sanitizar-string ""))
          (tc/map-columns :mn #(when % (if (int? %) % (->> % (re-seq #"\d") (apply str) Integer/parseInt))))) 
      (catch Exception e (let [ex {:mensaje (ex-message e)
                                   :datos (ex-data e)
                                   :causa (ex-cause e)}]
                           (prn "Hubo una excepción al parsear el archivo " ex)
                           (µ/log ::error-parseo-csv :mensaje ex)
                           (throw (ex-info "Hubo una excepción al parsear el archivo " ex (ex-cause e))))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; MAIN ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn -main
  "Invoke me with clojure -X:ingresar :perfil (:prod, :dev ó :test) :ruta (e.g. C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv) :print boolean :tbl-hist-txt true (defaults to false)"
  [& {:keys [perfil ruta print tbl-hist-txt] :or {tbl-hist-txt false}}]
  (iniciar-mulog perfil)
  (prn (str "Argumentos recibidos en main: " perfil " | " ruta " | " print " | " tbl-hist-txt))
  (µ/log ::iniciando-programa :argumentos {:perfil perfil :ruta ruta :print print})
  (when (some nil? [perfil ruta print])
    (µ/log ::invocacion-con-argumentos-insuficientes)
    (throw (ex-info "Invocar de la siguiente manera: clojure -X:ingresar :perfil (:prod, :dev ó :test) :ruta (e.g. \"C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv\") :print boolean :tbl-hist-txt true (false por defecto)"
                    {})))
  (let [conexiones (-> conf perfil)
        desal (:desal conexiones)
        asistencial (:asistencial conexiones)
        maestros (:maestros conexiones)]
    (try
      (with-open [asistencial-conn (jdbc/get-connection asistencial)
                  maestros-conn (jdbc/get-connection maestros)
                  desal-conn (jdbc/get-connection desal)]
        (when-let [data (-> (if print (leer-csv (str ruta) true) (leer-csv (str ruta))) normalizar-datos)]
          (let [ds (adjuntar-info-complementaria data asistencial-conn)
                ds-final (-> ds
                             (tc/select-columns [:historiaclinica
                                                 :da
                                                 :obra
                                                 :dnidelpaciente
                                                 :plan
                                                 :mn
                                                 :nro_afiliado
                                                 :horadeatencin
                                                 :nombredeldoctor
                                                 :nombredelpaciente
                                                 :fechadenacimiento
                                                 :coberturamdica
                                                 :seguimientoevolucin
                                                 :diagnstico
                                                 :motivo
                                                 :tratamiento])
                             (tc/rows :as-maps))]
            (if tbl-hist-txt
              (inserta-en-tablas-histpac-new ds-final desal-conn maestros-conn asistencial-conn)
              (inserta-en-tablas-histpac ds-final desal-conn maestros-conn asistencial-conn))))) 
      (catch Exception e (let [ex {:mensaje (ex-message e)
                                   :datos (ex-data e)
                                   :causa (ex-cause e)}]
                           (prn ex)
                           (µ/log ::error :excepcion ex))))))

(tests

 (sanitizar-string nil)

 (descomponer-hora 1852) := [18 52 0]
 (descomponer-hora 0) := [0 0 0]
 (descomponer-hora 1) := [0 1 0]
 (descomponer-hora 18) := [0 18 0]
 (descomponer-hora 100) := [1 0 0]

 (sumar-minutos 18 25 20) := [18 45]
 (sumar-minutos 18 55 20) := [19 15]
 (sumar-minutos 0 0 20) := [0 20]
 (sumar-minutos 14 5 20) := [14 25]
 (sumar-minutos 23 45 20) := [0 5]

 (sanitizar-string "GENTILE, LAURA") := "GENTILE, LAURA"
 (sanitizar-string "MARCO, SAÚL") := "MARCO, SAUL"
 (sanitizar-string "VITESSE, NÈ") := "VITESSE, NE"
 (sanitizar-string "VÊRT, MARCIN") := "VERT, MARCIN"
 (sanitizar-string "NÖEL, LAURA") := "NOEL, LAURA"
 (sanitizar-string "MARCO, SALÌ") := "MARCO, SALI"
 (sanitizar-string "MARCO SAM") := "MARCO SAM"
 (sanitizar-string "MARCO SAÜL") := "MARCO SAUL"
 (sanitizar-string "RENO, SÄULE") := "RENO, SAULE"
 (sanitizar-string "LINA, SÎM") := "LINA, SIM"
 (sanitizar-string "TÏM, LORENA") := "TIM, LORENA"
 (sanitizar-string "ËULER, YI") := "EULER, YI"
 (sanitizar-string "ÑAM, ÑAM") := "NAM, NAM"
 (sanitizar-string "DANIEL, ÑÕÑO") := "DANIEL, NONO"
 (sanitizar-string "VIÑAVAL DAGUM, MARIA ROSARIO (MN186964)") := "VINAVAL DAGUM, MARIA ROSARIO (MN186964)"
 (sanitizar-string "este es un texto en minúsculas, con eñes y cosas así, como ô ö y ò y  más óes") := "este es un texto en minusculas, con enes y cosas asi, como o o y o y  mas oes")


(comment

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; DEV SETUP ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (hyperfiddle.rcf/enable!)

  (clojure.repl.deps/sync-deps)

  (def p (p/open {:launcher :vs-code}))

  (add-tap #'p/submit)

   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; CONEXIONES ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (def desal-prod (-> conf :prod :desal))

  (def desal-dev (-> conf :dev :desal))

  (def maestros-dev (-> conf :dev :maestros))

  (def maestros-prod (-> conf :prod :maestros))

  (def asistencial-dev (-> conf :dev :asistencial))

  (def asistencial-prod (-> conf :prod :asistencial))

  (def asistencial-test (-> conf :test :asistencial))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (-main {:print false :ruta "C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv" :perfil :prod})
  
  (sql-busca-registro-en-tbc-histpac 12000 20240101 14 2)

  (with-open [conn (jdbc/get-connection asistencial-test)]
    (existe-registro? 885224 20240909 17 49 conn))

  (obtener-descripcion-diagnostico maestros-dev)

  (reset! descripcion-diagnostico nil)

  @descripcion-diagnostico

  (def csv (leer-csv "C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv"))

  (def csv (leer-csv "/home/jrivero/Telemedicina-presencial-Sanatorio.csv"))

  (def csv-2 (leer-csv "/home/jrivero/RocioFlores.csv"))

  (tap> (tc/info csv)) 
  
  (tap> (-> (tc/tail csv 50)
            (tc/print-dataset)))

  (def csv-transformado
    (-> csv-2 (tc/drop-missing [:da :horadeatencin])
        (tc/map-columns :da #(when %
                               (as-> % f
                                 (string/split f #"/")
                                 (reverse f)
                                 (apply str f)
                                 (Integer/parseInt f))))
        (tc/map-columns :fechadenacimiento #(when %
                                              (as-> % f
                                                (string/split f #"/")
                                                (reverse f)
                                                (apply str f)
                                                (Integer/parseInt f))))
        (tc/map-columns :horadeatencin #(-> %
                                            (string/replace #"\s|:|hs" "")
                                            Integer/parseInt))
        (tc/map-columns :nombredeldoctor #(-> % string/upper-case sanitizar-string))
        (tc/map-columns :nombredelpaciente #(-> % string/upper-case sanitizar-string))
        (tc/map-columns :seguimientoevolucin (fnil sanitizar-string ""))
        (tc/map-columns :diagnstico (fnil sanitizar-string ""))
        (tc/map-columns :motivo (fnil sanitizar-string ""))
        (tc/map-columns :tratamiento (fnil sanitizar-string ""))
        (tc/map-columns :mn #(when % (if (int? %) % (->> % (re-seq #"\d") (apply str) Integer/parseInt))))))

  (#(when % (->> % (re-seq #"\d") (apply str) Integer/parseInt)) "235645")

  (def normalizado (normalizar-datos csv))
  
  (tc/info normalizado)

  (tap> normalizado)

  (tc/map-columns normalizado :diagnstico sanitizar-string)

  (existe-registro? asistencial-prod 874113 20250204 15 04)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; PROCESAR tbc_reservas en memoria ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  

  (let [valid-date? (fn [d] (== 8 (-> d str count)))
        ds normalizado
        min-date (->> (tc/select-rows ds (comp valid-date? :da))
                      (tc/->array :da)
                      (apply min))
        reservas  (tc/dataset
                   (jdbc/execute! asistencial-prod (sql/format {:select [[:tbc_reservas/reservashiscli :historiaclinica]
                                                                         [:tbc_reservas/reservasfech :da]
                                                                         [:tbc_hist_cab_new/histcabobra :obra]
                                                                         [:tbc_hist_cab_new/histcabnrodoc :dnidelpaciente]
                                                                         [:tbc_hist_cab_new/histcabplanx :plan]
                                                                         [:tbc_hist_cab_new/histcabnrobenef :nro-afiliado]]
                                                                :from :tbc_reservas
                                                                :inner-join [[:tbc_hist_cab_new] [:= :tbc_reservas/reservashiscli :tbc_hist_cab_new/histcabnrounico]]
                                                                :where [:> :reservasfech min-date]}))
                   {:key-fn #(-> % name keyword)
                    :parser-fn {:historiaclinica :int32
                                :dnidelpaciente :int32}})]
    #_reservas
    #_(tc/inner-join reservas ds [:da :dnidelpaciente])
    #_(tap> (tc/left-join ds reservas [:da :dnidelpaciente]))
    #_(tc/left-join reservas ds [:da :dnidelpaciente])
    (-> (tc/left-join ds reservas [:da :dnidelpaciente]) #_(tc/right-join reservas ds [:da :dnidelpaciente])
        (tc/drop-rows (fn [row] (int? (:historiaclinica row))))
        (tc/select-columns [:da :horadeatencin :dnidelpaciente :nombredelpaciente])
        (tc/print-dataset {:print-line-policy :single})))
  
  (def reservas (obtener-ds-reservas asistencial-prod 20250101))

  (tc/info reservas)

  (tc/select-rows reservas (comp #{44363731} :dnidelpaciente))

  (tc/select-rows normalizado (comp #{32531145 43476960 44363731} :dnidelpaciente))

  (def d (adjuntar-info-complementaria normalizado asistencial-prod))
  
  (tc/select-rows d #(#{32531145 43476960 44363731} (:dnidelpaciente %)))
  
  (def d-2 (tc/unique-by d [:dnidelpaciente :historiaclinica :da :horadeatencin]))

  (tc/select-rows d (comp #{32531145 43476960 44363731} :dnidelpaciente))

  (tc/select-rows d-2 (comp #{32531145 43476960 44363731} :dnidelpaciente))
  
  (tc/tail d 50)

  (def dataset-final
    (with-open [conn (jdbc/get-connection #_asistencial-prod asistencial-dev)]
      (-> (adjuntar-info-complementaria normalizado conn)
          (tc/select-columns [:historiaclinica
                              :da
                              :obra
                              :dnidelpaciente
                              :plan
                              :nro_afiliado
                              :horadeatencin
                              :nombredeldoctor
                              :nombredelpaciente
                              :fechadenacimiento
                              :coberturamdica
                              :seguimientoevolucin
                              :diagnstico
                              :motivo
                              :mn
                              :tratamiento])
          (tc/rows :as-maps))))

  (tap> dataset-final)

  (def registros (mapv #(armar-registros-histpac-tbl-hist-txt % desal-dev maestros-dev) dataset-final))

  (def registros2 (mapv #(armar-registros-histpac % desal-dev maestros-dev) dataset-final)) 

  (tap> registros2)

  (tc/print-dataset dataset-final)

  (def registros-filtrados (filtrar-registros-existentes
                            #_(tap>) (mapv
                                      #(armar-registros-histpac % desal-prod maestros-prod)
                                      dataset-final)
                            asistencial-prod))

  (tap> registros-filtrados)

  (count registros-filtrados)

  (tap> (mapv (fn [[[hist fecha hora minutos]]]
                (existe-registro? asistencial-prod hist fecha hora minutos))
              registros-filtrados))

  (def historias (mapv first registros-filtrados))

  (tap> historias)

  (tap> (filter (fn [reg]
                  (== (first reg) 862673))
                historias))

  (def registro
    (-> d
        (tc/select-columns [:historiaclinica
                            :da
                            :obra
                            :dnidelpaciente
                            :plan
                            :mn
                            :nro_afiliado
                            :horadeatencin
                            :nombredeldoctor
                            :nombredelpaciente
                            :fechadenacimiento
                            :coberturamdica
                            :seguimientoevolucin
                            :diagnstico
                            :motivo
                            :tratamiento])
        (tc/rows :as-maps)))
  (inserta-en-tablas-histpac registro desal-prod maestros-prod asistencial-prod)
  (def v (armar-registros-histpac (first registro) desal-prod maestros-prod))

  (insertar-en-tbc-histpac [(first v)] asistencial-prod)
  (apply guarda-texto-de-historia asistencial-prod (first (v 1)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;  
  

  (drop 580 (tc/column normalizado :dnidelpaciente))

  (tc/select-rows normalizado (fn [row] (== (:dnidelpaciente row) 36068991)))

  (tc/info csv-transformado)

  (def fecha-nombre (-> (tc/select-columns csv-transformado [:da :dnidelpaciente])
                        (tc/rows)))

  (def historias-clinicas (->> d (mapv (fn [mp]
                                         (let [historia (some-> mp first (some [:reservashiscli :reservas-hiscli]))]
                                           ((fnil int 0) historia))))))

  (def datos-preparados (-> (tc/add-column csv-transformado :historiaclinica historias-clinicas)
                            (tc/rows :as-maps)))

  (with-open [des (jdbc/get-connection desal-dev)
              mas (jdbc/get-connection maestros-dev)
              asist (jdbc/get-connection asistencial-dev)]
    (let [reg (->  {:da 20240919,
                    :horadeatencin 1217,
                    :nombredeldoctor "SOLE, JUAN",
                    :mn 554889,
                    :nombredelpaciente "JOHANNA IAEL VANNI",
                    :dnidelpaciente 40903530,
                    :fechadenacimiento 19980113,
                    :coberturamdica "UNION PERSONAL",
                    :seguimiento "Se logra comunicación con el paciente, se le explica límites y alcances de la telemedicina y la importancia en la veracidad de los signos y síntomas referidos por el paciente para un buen proceder médico, paciente comprende y acepta. Paciente refiere cuadro de 5-7 dias de evolucion de tos productiva, afebril, sin disnea acompañado de leve odinofagia y congestion nasal. Niega otros sintomas", :diagnostico "Bronquitis aguda", :motivo "Dolor de garganta / Tos, Resfrío / Gripe / Dificultad Respiratoria, Dolor de oído, Dolor de Cabeza", :tratamiento "Niega alergias.Indico muco dosodos. Indico pautas de alarma las cuales paciente refiere comprender, control evolutivo en 24hs. Se indica interconsulta mediante atención presencial en caso de falta de mejoría de cuadro o empeoramiento sintomático.", :historiaclinica 886976, :obra 1820, :plan "ACCORD 220",
                    :nro-afiliado "0270544600"}
                   (armar-registros-histpac des mas)
                   first)]
      (jdbc/execute! asist (sql-inserta-en-tbc-histpac reg) {:builder-fn rs/as-unqualified-kebab-maps})))


  (with-open [des (jdbc/get-connection desal-dev)
              mas (jdbc/get-connection maestros-dev)
              asist (jdbc/get-connection asistencial-dev)]
    (-> (armar-registros-histpac {:da 20240920,
                                  :horadeatencin 1527,
                                  :nombredeldoctor "VALLE, GABRIEL",
                                  :mn 115669,
                                  :nombredelpaciente "DANIELA SILVINA MEDRANO",
                                  :dnidelpaciente 42682424,
                                  :fechadenacimiento 20000526,
                                  :coberturamdica "OSDE",
                                  :seguimiento "Cuadro Respiratorio Evolucio?n/Enfermedad actual: Se logra comunicacio?n con el paciente, se le explica li?mites y alcances de la 
                                                telemedicina y la importancia en la veracidad de los signos y si?ntomas referidos por el paciente para un buen proceder me?dico, 
                                                paciente comprende y acepta. irritacion ocular, secreciones se deriva a guardia de oftalmo NIEGA ALERGIA A MEDICACIO?N. 
                                                PAUTAS DE ALARMA, CONTROL CON ME?DICO DE CABECERA.",
                                  :diagnostico "Otros trastornos de la conjuntiva",
                                  :motivo "La paciente refiere dolor en los ojos se desperto? con ambo ojos pegados con lagañas y secreción blanca.",
                                  :tratamiento "se deriva a oftalmo",
                                  :historiaclinica 881118,
                                  :obra 1884,
                                  :plan "210",
                                  :nro-afiliado "61336593202"}
                                 des
                                 mas)
        first
        (insertar-en-tbc-histpac asist)))

  (def registros-test (with-open [des (jdbc/get-connection desal-dev)
                                  mas (jdbc/get-connection maestros-dev)]
                        (mapv #(armar-registros-histpac % des mas) procesados)))

  (def t (take 5 (map first registros-test)))

  (with-open [c (jdbc/get-connection asistencial-test)]
    (insertar-en-tbc-histpac (vec t) c))

  (first procesados)

  (count registros-test)

  (with-open [conn (jdbc/get-connection asistencial-test)]
    (filtrar-registros-existentes registros-test conn))

  \Ê
  \Ë
  \È
  \Ó
  \Í
  \Î
  \Ï
  \Ì
  \ß
  \Ô
  \Ò
  \õ
  \Õ

  (Character/charCount \Õ)

  (char 133)
  (char 230)
  (char 161)
  (char 140)

  (Character/getName 133)

  (map Character/getNumericValue "CARDINALI, CÉSAR")

  (->> (seq "CARDINALI, CÉSAR")
       (apply str))

  (->> (map #(clojure.string/replace % #"[^A-z\s,]" "") (seq "CARDINALI, CÉSAR"))
       (apply str))

  (map #(condp some [%]
          #{\Ê \Ë \È \É} :>> (constantly \E)
          #{\Á \À \Ã \Ä \Å} :>> (constantly \A)
          #{\Í \Ì \Î \Ï} :>> (constantly \I)
          #{\Ô \Ò \Õ \Ó \Ö} :>> (constantly \O)
          #{\Ù \Ú \Ü \Û} :>> (constantly \U)
          #{\Ç} :>> (constantly \C)
          identity :>> identity)
       (seq "CARDINALI, CÉSAR"))

;;;;;;;;;;;;;;;;;;;;; PREPARAR BASE DE DATOS DEL PERFIL TEST ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  ;; Asegurarse de poblar las bases de tbc_reservas y tbc_hist_cab_new con los datos adecuados!!!
  
  (with-open [conn (jdbc/get-connection asistencial-test)]
    (jdbc/execute! conn ["CREATE TABLE tbc_histpac (
	HistpacNro DECIMAL(10,0) NOT NULL,
	HistpacFec INTEGER NOT NULL,
	HistpacH INTEGER NOT NULL,
	HistpacM INTEGER NOT NULL,
	HistpacR INTEGER NOT NULL,
	HistpacE INTEGER NOT NULL,
	HistpacEspfir INTEGER NOT NULL,
	HistpacNro1 DECIMAL(10,0) NOT NULL,
	HistpacFec1 INTEGER NOT NULL,
	HistpacNro2 DECIMAL(10,0) NOT NULL,
	HistpacEspfir1 INTEGER NOT NULL,
	HistpacMedfir INTEGER,
	HistpacMotivo DECIMAL(10,0),
	HistpacEstudi DECIMAL(10,0),
	HistpacHoraSobre INTEGER,
	HistpacHFinal INTEGER,
	HistpacMFinal INTEGER,
	HistpacRFinal INTEGER,
	HistpacDiagno CHAR(28) NOT NULL,
	HistpacPatolo INTEGER,
	HistpacTratam DECIMAL(10,0),
	HistpacMedfirNya CHAR(30) NOT NULL,
	HistpacMedfirMat INTEGER,
	HistpacHAtenc INTEGER,
	HistpacMAtenc INTEGER,
	HistpacRAtenc INTEGER,
	HistpacDeriva INTEGER,
	HistpacDerivaDs INTEGER,
	HistpacDerivaSec CHAR(11) NOT NULL,
	HistpacObra INTEGER,
	HistpacPPlan CHAR(10) NOT NULL,
	HistpacPlan CHAR(1) NOT NULL,
	HistpacAfil CHAR(15) NOT NULL,
	HistpacPedAmbula INTEGER,
	HistpacConsHiv CHAR(1) NOT NULL,
	HistpacInterconsu INTEGER,
	HistpacEntregado INTEGER,
	HistpacCtro INTEGER,
	HistpacYodo CHAR(1) NOT NULL,
	HistpacCancd INTEGER,
	CONSTRAINT X_1_2_3_4_5_6 PRIMARY KEY (HistpacNro,HistpacFec,HistpacH,HistpacM,HistpacR,HistpacE)
);"])
    (jdbc/execute! conn ["CREATE TABLE tbc_histpac_txt (
	Txt1 DECIMAL(10,0) NOT NULL,
	Txt1g INTEGER NOT NULL,
	Txt2 INTEGER NOT NULL,
	Txt3 INTEGER NOT NULL,
	Txt4 CHAR(78) NOT NULL,
	Txt6 INTEGER,
	CONSTRAINT X_1_2_3_4 PRIMARY KEY (Txt1,Txt1g,Txt2,Txt3)
);"]))

  (with-open [conn (jdbc/get-connection asistencial-test)]
    (jdbc/execute! conn ["CREATE TABLE tbc_reservas (
	ReservasEsp INTEGER,
	ReservasMed INTEGER,
	ReservasFech INTEGER,
	ReservasHora INTEGER,
	ReservasMed1 INTEGER,
	ReservasEsp1 INTEGER,
	ReservasFech1 INTEGER,
	ReservasHora1 INTEGER,
	ReservasTipo CHAR(1) NOT NULL,
	ReservasEsp2 INTEGER,
	ReservasMed2 INTEGER,
	ReservasFech2 INTEGER,
	ReservasHora2 INTEGER,
	ReservasEsta INTEGER,
	ReservasFech3 INTEGER,
	ReservasMed3 INTEGER,
	ReservasEsp3 INTEGER,
	ReservasHora3 INTEGER,
	ReservasFech6 INTEGER,
	ReservasMed6 INTEGER,
	ReservasEsp6 INTEGER,
	ReservasHora6 INTEGER,
	ReservasHiscli DECIMAL(10,0),
	ReservasFech5 INTEGER,
	ReservasHora5 INTEGER,
	ReservasObra INTEGER,
	ReservasObrpla CHAR(10) NOT NULL,
	ReservasNroben CHAR(15) NOT NULL,
	ReservasNumeroFc INTEGER,
	ReservasLetraFc CHAR(1) NOT NULL,
	ReservasMedsol INTEGER,
	ReservasTipsol CHAR(1) NOT NULL,
	ReservasAtendi INTEGER,
	ReservasPrimeraVez INTEGER,
	ReservasTipser INTEGER,
	ReservasIPedido INTEGER,
	ReservasHoraAtenc INTEGER,
	ReservasHoraSobre INTEGER,
	ReservasOpera INTEGER,
	ReservasTipoOpera INTEGER,
	ReservasFechaPed INTEGER,
	ReservasHoraPed INTEGER,
	ReservasSexo INTEGER,
	ReservasOperaConf INTEGER,
	ReservasTipoOperaConf INTEGER,
	ReservasCron INTEGER,
	ReservasLlamadaEstado CHAR(1) NOT NULL,
	ReservasLlamadaDestino INTEGER,
	ReservasIva INTEGER,
	ReservasTipserCef INTEGER,
	ReservasEstaCef INTEGER,
	ReservasFechCef INTEGER,
	ReservasAtendiCef INTEGER,
	ReservasHoraCef INTEGER,
	ReservasServicio INTEGER,
	ReservasMedicab CHAR(13) NOT NULL,
	ReservasAutoriza CHAR(12) NOT NULL,
	ReservasConsu INTEGER,
	ReservasPartEspe INTEGER,
	ReservasConEstudio INTEGER,
	ReservasETipoEstudio_10 INTEGER,
	ReservasETipoEstudio_9 INTEGER,
	ReservasETipoEstudio_8 INTEGER,
	ReservasETipoEstudio_7 INTEGER,
	ReservasETipoEstudio_6 INTEGER,
	ReservasETipoEstudio_5 INTEGER,
	ReservasETipoEstudio_4 INTEGER,
	ReservasETipoEstudio_3 INTEGER,
	ReservasETipoEstudio_2 INTEGER,
	ReservasETipoEstudio_1 INTEGER,
	ReservasMenorAcom INTEGER,
	ReservasFechaConf INTEGER,
	ReservasHoraConf INTEGER,
	ReservasNordEges CHAR(12) NOT NULL,
	CONSTRAINT X_reservas PRIMARY KEY (ReservasEsp,ReservasMed,ReservasFech,ReservasHora)
);"]))

  (with-open [conn (jdbc/get-connection asistencial-test)]
    (jdbc/execute! conn ["CREATE TABLE tbc_hist_cab_new (
	HistCabNroUnico DECIMAL(10,0) NOT NULL,
	HistCabSexo INTEGER NOT NULL,
	HistCabFechaNac INTEGER NOT NULL,
	HistCabCharsApe CHAR(8) NOT NULL,
	HistCabTipoDoc INTEGER NOT NULL,
	HistCabNroDoc DECIMAL(10,0) NOT NULL,
	HistCabApellido CHAR(20) NOT NULL,
	HistCabNombres CHAR(20) NOT NULL,
	HistCabFecAten INTEGER NOT NULL,
	HistCabNroUnico1 DECIMAL(10,0) NOT NULL,
	HistCabObra INTEGER NOT NULL,
	HistCabPlanX CHAR(10) NOT NULL,
	HistCabApellNom CHAR(40) NOT NULL,
	HistCabNroBenef CHAR(15) NOT NULL,
	HistCabApeCasada CHAR(20) NOT NULL,
	HistCabEstCivil INTEGER,
	HistCabIntObsAmd INTEGER,
	HistCabIntObsHor INTEGER,
	HistCabEsPersonal INTEGER,
	CONSTRAINT X_1 PRIMARY KEY (HistCabNroUnico)
);"]))

  (with-open [conn (jdbc/get-connection asistencial-test)]
    (jdbc/execute! conn ["SELECT * FROM tbc_histpac"])
    #_(jdbc/execute! conn ["SELECT * FROM tbc_histpac_txt"])
    #_(jdbc/execute! conn ["SELECT * FROM tbc_reservas"])
    #_(jdbc/execute! conn ["SELECT COUNT(*) FROM tbc_hist_cab_new"]))

  (with-open [conn (jdbc/get-connection asistencial-prod)]
    (jdbc/execute! conn (consulta-en-tbc-reservas 20250104 4619490) {:builder-fn rs/as-unqualified-kebab-maps}))

  (sumar-minutos 15 4 20)
  :rfc
  ) 