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
   [com.brunobonacci.mulog :as µ])
  (:import (java.sql SQLException
                     SQLIntegrityConstraintViolationException
                     SQLTimeoutException
                     SQLClientInfoException)
           java.time.LocalDateTime)) 

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

(defn consulta-en-tbc-reservas
  [fecha dni]
  ["SELECT r.reservashiscli, n.histcabobra, n.histcabplanx, n.histcabnrobenef 
    FROM tbc_reservas r
    INNER JOIN tbc_hist_cab_new n ON r.reservashiscli = n.histcabnrounico
    WHERE r.reservasfech = ? AND n.histcabnrodoc = ?" fecha dni])

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

(defn buscar-historias-clinicas
  [ds conexion]
  (prn "Buscando números de historia clínica...")
  (µ/log ::busqueda-historias-clinicas)
  (let [v (-> (tc/select-columns ds [:da :dnidelpaciente])
              (tc/rows))] 
    (try
      (mapv (fn [[fec dni]] 
              (as-> (consulta-en-tbc-reservas fec dni) stmt
                (jdbc/execute! conexion stmt {:builder-fn rs/as-unqualified-kebab-maps})
                 (if (empty? stmt)
                   (do (prn "No se encontró historia para el paciente con dni " dni " con fecha de " fec)
                       (µ/log ::historias-clinicas-no-encontradas :mensaje "No se encontró historia para el paciente" :dni dni :fecha fec)
                       stmt)
                   stmt)))
            v)
      (catch SQLException e (throw (let [msj (ex-message e)] 
                                     (µ/log ::error-busqueda-historias-clinicas :mensaje msj)
                                     (prn msj)
                                     (ex-info "Error al recuperar las historias clínicas" {:mensaje msj} (ex-cause e))))))))

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
      (catch SQLException e (throw (let [msj (ex-message e)]
                                     (µ/log ::error-guardado-texto-historia :mensaje msj)
                                     (ex-info "Error al guardar texto de historia: " {:conexion conn
                                                                                      :numerador numerador
                                                                                      :texto texto
                                                                                      :mensaje msj} 
                                              (ex-cause e))))))))

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
      407
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

(defn insertar-en-tbc-histpac
  [registros conexion]
  (prn "Insertando registros en tbc_histpac...")
  (µ/log ::insercion-tbc-histpac)
  (when (== 0 (count registros))
    (µ/log ::no-existen-registros-para-insertar)
    (throw (ex-info "No existen registros para insertar" {:registros registros})))
  (try 
    (let [cantidad (atom 0)]
      (doseq [registro registros] (when (-> (jdbc/execute! conexion (sql-inserta-en-tbc-histpac registro) {:builder-fn rs/as-unqualified-kebab-maps})
                                            first
                                            :next.jdbc/update-count)
                                    (swap! cantidad inc)))
      (prn (str @cantidad " registro(s) insertado(s)"))
      (µ/log ::registros-insertados :cantidad @cantidad))
    (catch SQLException e (let [msj (ex-message e)] 
                            (prn (str "Hubo un problema al insertar en tbc_histpac " msj))
                              (µ/log ::error-insercion-tbc-histpac :mensaje msj)
                            (throw (ex-info "Error al insertar en tbc_histpac" {:mensaje msj} (ex-cause e)))))))

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
  "Recibe un vector de vectores como el producido por `armar-registros-histpac` y devuelve una vector con las mismas propiedades con excepción de 
   los vectores que contienen la información de los registros ya insertados"
 [registros-vec conexion-asistencial]
  (prn "Buscando registros ya guardados...")
  (µ/log ::busqueda-registros-ya-existentes)
  (doall (remove (fn [[histpac]]
                   (apply existe-registro? conexion-asistencial (take 4 histpac))) 
                 registros-vec)))

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
          (tc/map-columns :seguimientoevolucin #(sanitizar-string %))
          (tc/map-columns :diagnstico #(sanitizar-string %))
          (tc/map-columns :motivo #(sanitizar-string %))
          (tc/map-columns :tratamiento #(sanitizar-string %)))
      (catch ClassCastException e (let [msj (ex-message e)] 
                                    (prn "Hubo una excepción al parsear el archivo " msj)
                                    (µ/log ::error-parseo-csv :mensaje msj)
                                    (throw (ex-info "Hubo una excepción al parsear el archivo " {:mensaje msj} (ex-cause e)))))
      (catch Exception e (let [msj (ex-message e)] 
                           (prn "Hubo una excepción al parsear el archivo " msj)
                           (µ/log ::error-parseo-csv :mensaje msj)
                           (throw (ex-info "Hubo una excepción al parsear el archivo " {:mensaje msj} (ex-cause e))))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; MAIN ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn -main
  "Invoke me with clojure -X:ingresar :perfil (:prod, :dev ó :test) :ruta (e.g. C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv) :print boolean"
  [& {:keys [perfil ruta print]}]
  (iniciar-mulog perfil)
  (prn (str "Argumentos recibidos en main: " perfil " | " ruta " | " print))
  (µ/log ::iniciando-programa :argumentos {:perfil perfil :ruta ruta :print print})
  (when (some nil? [perfil ruta print])
    (µ/log ::invocacion-con-argumentos-insuficientes)
    (throw (ex-info "Invocar de la siguiente manera: clojure -X:ingresar :perfil (:prod, :dev ó :test) :ruta (e.g. \"C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv\") :print boolean"
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
          (let [hist-cab (buscar-historias-clinicas data asistencial-conn)
                historias (->> hist-cab (mapv (fn [mp]
                                                (let [historia (some-> mp first (some [:reservashiscli :reservas-hiscli]))]
                                                  ((fnil int 0) historia)))))
                obras (->> hist-cab (mapv #(-> % first :hist-cab-obra)))
                planes (->> hist-cab (mapv #(-> % first :hist-cab-plan-x ((fnil string/trim "")))))
                nros-afiliado (->> hist-cab (mapv #(-> % first :hist-cab-nro-benef ((fnil string/trim "")))))
                ds-final (-> (tc/add-columns data {:historiaclinica historias
                                                   :obra obras
                                                   :plan planes
                                                   :nro-afiliado nros-afiliado})
                             (tc/drop-rows #(== (:historiaclinica %) 0))
                             (tc/rows :as-maps))]
             (inserta-en-tablas-histpac ds-final desal-conn maestros-conn asistencial-conn))))
      (catch SQLTimeoutException e (let [msj (ex-message e)] 
                                     (prn msj)
                                     (µ/log ::error :mensaje msj)))
      (catch SQLClientInfoException e (let [msj (ex-message e)] 
                                        (prn msj)
                                        (µ/log ::error :mensaje msj)))
      (catch SQLIntegrityConstraintViolationException e (let [msj (ex-message e)] 
                                                          (prn msj)
                                                          (µ/log ::error :mensaje msj)))
      (catch SQLException e (let [msj (ex-message e)] 
                              (prn msj)
                              (µ/log ::error :mensaje msj)))
      (catch Exception e (let [msj (ex-message e)
                               data (ex-data e)] 
                           (prn (ex-message e) (ex-data e))
                           (µ/log ::error :mensaje msj :datos data))))))

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
 (sanitizar-string "este es un texto en minúsculas, con eñes y cosas así, como ô ö y ò y  más óes") := "este es un texto en minusculas, con enes y cosas asi, como o o y o y  mas oes"
 )

  
(comment

  (hyperfiddle.rcf/enable!)
  (clojure.repl.deps/sync-deps)

   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; CONEXIONES ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (def desal-prod (-> conf :prod :desal))

  (def desal-dev (-> conf :dev :desal))

  (def maestros-dev (-> conf :dev :maestros))

  (def maestros-prod (-> conf :prod :maestros))

  (def asistencial-dev (-> conf :dev :asistencial))

  (def asistencial-prod (-> conf :prod :asistencial))

  (def asistencial-test (-> conf :test :asistencial))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  
  (sql-busca-registro-en-tbc-histpac 12000 20240101 14 2)

  (with-open [conn (jdbc/get-connection asistencial-test)]
    (existe-registro? 885224 20240909 17 49 conn))

  (obtener-descripcion-diagnostico maestros-dev)

  (reset! descripcion-diagnostico nil)

  @descripcion-diagnostico

  (def csv (leer-csv "C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv"))

  (def csv (leer-csv "/home/jrivero/Telemedicina-presencial-Sanatorio.csv"))

  (tc/info csv) 
  
  (-> csv 
      (tc/select-columns [:mn])
       (tc/select-rows #(string? (:mn %))))
  
  (-> csv 
      (tc/select-rows #(= (:mn %) "20321)")))

  (tc/head csv)
  
  (tc/select csv [:da :horadeatencin :nombredeldoctor :nombredelpaciente] (fn [row] (= (:horadeatencin row) "14:25 h")))

  (def csv-transformado (-> csv
                            (tc/drop-missing [:da :horadeatencin])
                            (tc/map-columns :da #(as-> % f
                                                   (string/split f #"-")
                                                   (reverse f)
                                                   (apply str f)
                                                   (Integer/parseInt f)))
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
                            (tc/map-columns :seguimientoevolucin #(sanitizar-string %))
                            (tc/map-columns :diagnstico #(sanitizar-string %))
                            (tc/map-columns :motivo #(sanitizar-string %))
                            (tc/map-columns :tratamiento #(sanitizar-string %))))

  (def normalizado (normalizar-datos csv))

   (drop 580 (tc/column normalizado :dnidelpaciente))

  (tc/select-rows normalizado (fn [row] (== (:dnidelpaciente row) 36068991)))
                  
  (tc/info csv-transformado)

  (def fecha-nombre (-> (tc/select-columns csv-transformado [:da :dnidelpaciente])
                        (tc/rows)))
  
  (def d (with-open [conn (jdbc/get-connection #_asistencial-test asistencial-prod #_asistencial-dev)]
           (buscar-historias-clinicas normalizado #_csv-transformado conn)))

  (->> d
       (map empty?)
       (remove false?)
       count)

  (map (fn [[fec dni]]
         (consulta-en-tbc-reservas fec dni))
       fecha-nombre)

  (def historias-clinicas (->> d (mapv (fn [mp]
                                         (let [historia (some-> mp first (some [:reservashiscli :reservas-hiscli]))]
                                           ((fnil int 0) historia))))))

  (some {:reservas-hiscli 740254M,
         :hist-cab-obra 1820,
         :hist-cab-plan-x "CLASSIC   ",
         :hist-cab-nro-benef "0078111502     "} [:reservashiscli :reservas-hiscli])

  (some #{:reservashiscli :reservas-hiscli} (seq {:reservas-hiscli 740254M,
                                                  :hist-cab-obra 1820,
                                                  :hist-cab-plan-x "CLASSIC   ",
                                                  :hist-cab-nro-benef "0078111502     "}))

  (def obra (->> d (mapv #(-> % first :hist-cab-obra))))

  (def plan (->> d (mapv #(-> % first :hist-cab-plan-x ((fnil string/trim ""))))))

  (def nroafiliado (->> d (mapv #(-> % first :hist-cab-nro-benef ((fnil string/trim ""))))))

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

  (def procesados (try
                    (with-open [asistencial-conn (jdbc/get-connection asistencial-test)]
                      (let [data (normalizar-datos (leer-csv "C://Users//jrivero//Downloads//Telemedicina-presencial-Sanatorio.csv"))
                            hist-cab (buscar-historias-clinicas data asistencial-conn)
                            historias (->> hist-cab (mapv #(-> % first #_:reservas-hiscli :reservashiscli ((fnil int 0)))))
                            obras (->> hist-cab (mapv #(-> % first :hist-cab-obra)))
                            planes (->> hist-cab (mapv #(-> % first :hist-cab-plan-x ((fnil string/trim "")))))
                            nros-afiliado (->> hist-cab (mapv #(-> % first :hist-cab-nro-benef ((fnil string/trim "")))))
                            ds-final (-> (tc/add-columns data {:historiaclinica historias
                                                               :obra obras
                                                               :plan planes
                                                               :nro-afiliado nros-afiliado})
                                         (tc/drop-rows #(== (:historiaclinica %) 0))
                                         (tc/rows :as-maps))]
                        ds-final))
                    (catch Exception e (ex-data e))))


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


  :rfc
  ) 