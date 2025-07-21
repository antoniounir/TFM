#!/usr/bin/env python
# coding: utf-8

# # TFM - Funciones para la evaluación del la calidad - FuncionesTFM.Py

# ### Datos del autor
# Notebook elaborado por: **César Fernando Balaguer García** y **Antonio Luís Almira Martínez** en el marco de la tesis de Máster Universitario en **Análisis y Visualización de Datos Masivos / Visual Analytic y Big Data** (2025) <br>
# Asesora de tesis: **María Belén Benalcázar Tovar**. <br>
# Título: **Marco Metodológico que permita medir la calidad de los datos de un conjunto de datos en formatos planos**.

# Este script de python tiene todas las funciones necesarias para realizar la validación de los datos de un conjunto de datos, existe una función por cada dimensión de calidad

# ## INICIALIZACIÓN
# Esta sección importa las librerías inicializa las variables necesariase para poder realziar la metodología de evaluación de la calidad de un conjunto de datos

# In[1]:


import pandas as pd
import numpy as np
import importlib
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import ast
import re
import sweetviz as sv

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path

from IPython.display import display, clear_output
from matplotlib.backends.backend_pdf import PdfPages
from math import pi


# In[2]:


mensaje_rojo     = "El porcentaje de calidad se encuentra por debajo del umbral mínimo"
mensaje_amarillo = "El porcentaje de calidad se encuentra por debajo del umbral aceptable"
mensaje_verde    = "El porcentaje de calidad sobre el umbral aceptable"

dimensiones = [
    "Exactitud",
    "Completitud",
    "Consistencia",
    "Credibilidad",
    "Unicidad"
]

configuraciones = []
respuesta = False
nombre_archivo = None


# ## SECCIÓN DE DEFINICIÓN DE FUNCIONES DE APOYO
# En esta sección se define una función por cada dimensión de calidad a evaluar, si se desean más funciones se deberán incluir en la colección inicial creada líneas arriba, estas funciones devolveran un valor porcentual la cual indicará cuantos datos cumplen con la dimensión evaluada para el campo que se está evaluando, este último dato lo recibirá como parámetro, al igual que los umbrales mínimo y aceptable.

# #### Función FORMATO_A_REGEX
# Esta función permite cambiar el campo validador registrado por el usuario a un formato que Python pueda entender, se utiliza para algunas de las validaciones de las dimensiones de calidad

# In[3]:


import re

def FORMATO_A_REGEX(validador):
    # ————— Fechas —————
    if validador == "yyyy-mm-dd":
        return r"^\d{4}-\d{2}-\d{2}$"
    if validador == "dd-mm-yyyy":
        return r"^\d{2}-\d{2}-\d{4}$"
    if validador == "dd/mm/yyyy":
        return r"^\d{2}/\d{2}/\d{4}$"
    if validador == "yyyy/mm/dd":
        return r"^\d{4}/\d{2}/\d{2}$"
    if validador == "yyyymmdd":
        return r"^\d{8}$"
    if validador == "yymmdd":
        return r"^\d{6}$"
    if validador == "yyyy":
        return r"^\d{4}$"

    # ————— Email —————
    if validador == "email":
        return r"^[\w\.-]+@[\w\.-]+\.\w{2,}$"

    # ————— Texto tipo Xn —————
    if re.fullmatch(r"X\d+", validador):
        n = int(validador[1:])
        return rf"^.{{0,{n}}}$"

    # ————— Formato específico: ########9[.99] —————
    if validador == "########9[.99]":
        return r"^-?\d{1,9}(\.\d{2})?$"

    if validador == "########9[.9#]":
        return r"^-?\d{1,9}(\.\d{1,2})?$"

    if validador == "########9[.9##]":
        return r"^-?\d{1,9}(\.\d{1,3})?$"

    if validador == "########9[.9###]":
        return r"^-?\d{1,9}(\.\d{1,4})?$"

    if validador == "########9[.9####]":
        return r"^-?\d{1,9}(\.\d{1,5})?$"

    if validador == "########9[.9#####]":
        return r"^-?\d{1,9}(\.\d{1,6})?$"

    if validador == "########9[.##]":
        return  r"^\d{1,9}(\.\d{2})?$"

    if validador == "########9[,##]":
        return r"^\d{1,9}(,\d{2})?$"

    if validador == "###,###,##9[.######]":
        return r"^(\d{1,3}(,\d{3})*|\d+)(\.\d{1,6})?$"

    if validador == "###,###,##9[.#####]":
        return r"^(\d{1,3}(,\d{3})*|\d+)(\.\d{1,5})?$"

    if validador == "###,###,##9[.####]":
        return r"^(\d{1,3}(,\d{3})*|\d+)(\.\d{1,4})?$"

    if validador == "###,###,##9[.###]":
        return r"^(\d{1,3}(,\d{3})*|\d+)(\.\d{1,3})?$"

    if validador == "###,###,##9[.##]":
        return r"^(\d{1,3}(,\d{3})*|\d+)(\.\d{1,2})?$"

    if validador == "###,###,##9[.#]":
        return r"^(\d{1,3}(,\d{3})*|\d+)(\.\d{1,1})?$"

    if validador == "###,###,##9":
        return r"^(\d{1,3}(,\d{3})*|\d+)*$"

    if validador == "###.###.##9[,######]":
        return r"^(\d{1,3}(\.\d{3})*|\d+)(,\d{1,6})?$"

    if validador == "###.###.##9[,#####]":
        return r"^(\d{1,3}(\.\d{3})*|\d+)(,\d{1,5})?$"

    if validador == "###.###.##9[,####]":
        return r"^(\d{1,3}(\.\d{3})*|\d+)(,\d{1,4})?$"

    if validador == "###.###.##9[,###]":
        return r"^(\d{1,3}(\.\d{3})*|\d+)(,\d{1,3})?$"

    if validador == "###.###.##9[,##]":
        return r"^(\d{1,3}(\.\d{3})*|\d+)(,\d{1,2})?$"

    if validador == "###.###.##9[,#]":
        return r"^(\d{1,3}(\.\d{3})*|\d+)(,\d{1,1})?$"

    if validador == "###.###.##9":
        return r"^(\d{1,3}(\.\d{3})*|\d+)*$"

    # ————— Otros formatos numéricos similares —————
    if re.fullmatch(r"[#,9]+(\.\d+)?(\[\.\d+\])?", validador):
        base = validador
        decimal_opcional = False
        decimales = 0

        if '[.' in base:
            base, parte_decimal = base.split('[.', 1)
            decimales = parte_decimal.strip(']').count('9')
            decimal_opcional = True
        elif '.' in base:
            base, parte_decimal = base.split('.', 1)
            decimales = parte_decimal.count('9')

        cantidad = base.count('9') + base.count('#')
        entero_regex = rf"\d{{1,{cantidad}}}"

        if decimales > 0:
            decimal_regex = rf"\.\d{{{decimales}}}"
            if decimal_opcional:
                decimal_regex = rf"(?:{decimal_regex})?"
        else:
            decimal_regex = ""

        return rf"^{entero_regex}{decimal_regex}$"

    # ————— Ningún caso válido —————
    raise ValueError(f"❌ Formato de validador no reconocido: '{validador}'")



# #### Función OBTENER_SEMAFORO
# Esta función evalúa un porcentaje de calidad recibido y lo ubica dentro del umbral correspondiente, devolviendo el color del semáforo que le corresponde, es una función de apoyo utilizada al momento de generar el Dashboard final

# In[4]:


def OBTENER_SEMAFORO(calidad, umbral_minimo, umbral_aceptable):
    if calidad >= umbral_aceptable:
        return '●', 'green'
    elif calidad >= umbral_minimo:
        return '●', 'yellow'
    else:
        return '●', 'red'


# #### Fucnción IMPRIMIR_TITULO
# Esta función permite imprimir el título de sección en pantalla, se utiliza tanto en la parametría como en la evaluación y el resultado final por lo que se establece como una función de apoyo

# In[5]:


def IMPRIMIR_TITULO(texto):
    print(f"\033[1m\033[4m{texto}\033[0m")  # Negrita + subrayado


# #### Función IMPRIMIR_RESULTADO
# Función utilizada para imprimir en pantalla el resultado de la evaluación de la calidad de un conjunto de datos 

# In[6]:


def IMPRIMIR_RESULTADO(icono, campo, dimension, calidad, umbral_minimo, umbral_aceptable, mensaje):
    # Si 'campo' es lista, la unimos en un string
    if isinstance(campo, list):
        campo = ", ".join(campo)

    # Anchos fijos para cada columna
    ancho_icono        = 2
    ancho_campo        = 25
    ancho_dimension    = 15
    ancho_calidad      = 10
    ancho_umbral_min   = 13
    ancho_umbral_acp   = 14

    print(
        f"{icono:<{ancho_icono}}  "
        f"{campo[:ancho_campo-2]:<{ancho_campo}} | "
        f"{dimension[:ancho_dimension-2]:<{ancho_dimension}} | "
        f"{calidad:>{ancho_calidad}.2f}% | "
        f"{umbral_minimo:>{ancho_umbral_min}.2f}% | "
        f"{umbral_aceptable:>{ancho_umbral_acp}.2f}% | "
        f"{mensaje}"
    )


# #### Función IMPRIMIR_PARAMETRÍA
# Función que permite imprimir la parametría que se ha configurado para un conjunto de datos

# In[7]:


def IMPRIMIR_PARAMETRIA(configuraciones):
    IMPRIMIR_TITULO("CONFIGURACIÓN REGISTRADA")
    print("")
    print(f"{'Encabezado':<25} {'Dimensión':<15} {'Validador':<45} {'Umbral Min':>10} {'Umbral Acep':>10}")
    print("-" * 105)

    for config in configuraciones:
        encabezado = ", ".join(config['encabezado'])
        print(f"{encabezado[:23]:<25} {config['dimension']:<15} {config['validador'][:43]:<45} {config['umbral_minimo']:>10} {config['umbral_aceptable']:>10}")



# #### función LEER_CSV
# Función que permite leer un archivo en formato CSV

# In[8]:


def LEER_CSV(filepath, separador):
    codificaciones = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
    for encoding in codificaciones:
        try:
            df = pd.read_csv(filepath, sep=separador, encoding=encoding)
            print(f"✅ Archivo leído con codificación: {encoding}")
            return df
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("❌ No se pudo leer el archivo con las codificaciones comunes.")


# ## SECCIÓN FASE 1 DE LA METODOLOGÍA - CARGA DEL CONJUNTO DE DATOS A EVALUAR (ENTRADA)
# En esta sección se encuentran todas las funciones para la selección y carga del archivo a evaluar, la cual corresponde a la **Fase 1** de la metodología

# #### Función SELECCIONA_CONJUNTO DE DATOS
# Permite seleccionar de la ruta actual un conjunto de datos en formato csv con el fin de poder realizar la parametría y posterior evaluación de la calidad

# In[9]:


def SELECCIONA_CONJUNTO_DATOS():


    initial_dir = str(Path(__file__).parent.resolve())
    nombre_archivo = None
    # ---------- Funciones internas ----------
    def BUSCAR_CSV_EN_CARPETA(carpeta: Path):
        return [f.name for f in carpeta.glob("*.csv")]

    def SELECCIONAR_CARPETA():
        nueva_ruta = filedialog.askdirectory()
        if nueva_ruta:
            ruta_carpeta.set(nueva_ruta)
            ACTUALIZAR_COMBOBOX()

    def ACTUALIZAR_COMBOBOX():
        carpeta = Path(ruta_carpeta.get())
        lbl_ruta.config(text=f"Carpeta actual:\n{carpeta}")
        archivos = BUSCAR_CSV_EN_CARPETA(carpeta)
        if archivos:
            combo["values"] = archivos
            combo.set(archivos[0])
        else:
            if messagebox.askyesno(
                "No hay CSV",
                "No se encontraron archivos CSV en esta carpeta.\n¿Quieres elegir otra?"
            ):
                SELECCIONAR_CARPETA()
            else:
                combo["values"] = []
                combo.set("")

    def ACEPTAR():
        nonlocal nombre_archivo
        archivo = combo.get()
        if archivo:
            nombre_archivo = Path(archivo).stem
            root.destroy()
        else:
            messagebox.showwarning("Sin selección", "Debes escoger un archivo primero.")

    # ---------- Interfaz ----------
    root = tk.Tk()
    root.title("SELECCIONA UN CONJUNTO DE DATOS")
    root.geometry("500x220")

    ruta_carpeta = tk.StringVar(value=initial_dir)

    lbl_ruta = ttk.Label(root, text="", wraplength=480, justify="center", foreground="gray")
    lbl_ruta.pack(pady=(10, 5))

    btn_cambiar = ttk.Button(root, text="Cambiar carpeta", command=SELECCIONAR_CARPETA)
    btn_cambiar.pack(pady=5)

    combo = ttk.Combobox(root, state="readonly")
    combo.pack(padx=20, pady=5, fill="x")

    btn_aceptar = ttk.Button(root, text="Aceptar", command=ACEPTAR)
    btn_aceptar.pack(pady=10)

    ACTUALIZAR_COMBOBOX()  

    root.mainloop()
    return nombre_archivo


# #### Funión CARGA_DATASET
# Esta función realiza la carga del conjunto de datos el cual se va a parametrizar, una vez realizada la carga evalúa si existe una configuración previa y la muestra en pantalla, consulta si desea reemplazar el archivo existente o si desea añadir nuevos registros

# In[10]:


def CARGA_DATASET(nombre_archivo):
    configuraciones = []
    #nombre_archivo
    #nombre_archivo = input("\nIngrese el nombre del archivo de datos a evaluar (sin extensión): ")
    nombre_archivo = SELECCIONA_CONJUNTO_DATOS()
    if nombre_archivo != "":
        archivo = nombre_archivo + ".csv"
        df = LEER_CSV(archivo, ";")
        encabezados = list(df.columns)
        clear_output(wait=True)

        IMPRIMIR_TITULO("Conjunto de datos y nombre de campos a evaluar")
        print("")
        print(archivo)
        print(encabezados)

        archivo_conf = nombre_archivo + ".conf"
        if Path(archivo_conf).exists():
            df_parametro = LEER_CSV(archivo_conf, ";")
            df_parametro['encabezado'] = df_parametro['encabezado'].apply(ast.literal_eval)
            configuraciones = df_parametro.to_dict(orient="records")
            IMPRIMIR_PARAMETRIA(configuraciones)
            respuesta = messagebox.askyesno(
            "Archivo existente",
            f"El archivo '{archivo_conf}' ya existe.\n\n¿Deseas AÑADIR al contenido existente?\n(Sí = Añadir, No = Reemplazar)"
            )
            if not respuesta:
                configuraciones = []
        else:
            respuesta = False
        return nombre_archivo, encabezados, configuraciones, respuesta
    else:
        IMPRIMIR_TITULO("NO SELECCIONÓ NINGÚN ARCHIVO")
        return "", "", configuraciones, Respuesta



# ## SECCIÓN FASE 2 DE LA METODOLOGÍA - EVALUACIÓN DE LA CALIDAD DEL CONJUNTO DE DATOS (PROCESO)
# En esta sección se encuentran todas las funciones necesarias para la evaluación de la calidad, para ello la metodología consta de 2 partes, la primrea correspondiente a la configuración de la paametría y la segunda al procesamiento propia de la evaluación de la calidad

# ### PARTE 1: PARAMETRÍA
# En esta parte se incluye todas las funciones necesarias para realizar la parametría

# #### Función REGISTRA_PARAMETRIA
# Esta función permite registrar toda la parametría necesaria para la validación de la calidad

# In[11]:


def REGISTRA_PARAMETRIA(nombre_archivo, encabezados, configuraciones, respuesta):
    if not respuesta:
        configuraciones = []

    IMPRIMIR_TITULO("Registre los datos necesarios para realizar la validación del conjunto de datos")

    while True:
        entrada = input("\nSelecciona un nombre de campo o varios (ejemplo: campo1 o [\"campo1\", \"campo2\"] a evaluar) (deja en blanco para terminar): ")

        if entrada.strip() == "":
            break

        try:
            encabezado = ast.literal_eval(entrada) if entrada.strip().startswith("[") else entrada.strip()
        except Exception:
            print("Formato inválido. Si deseas ingresar varios campos usa el formato: [\"campo1\", \"campo2\"]")
            continue

        if isinstance(encabezado, str):
            encabezado = [encabezado]

        if not all(campo in encabezados for campo in encabezado):
            print("Uno o más encabezados no válidos. Intenta de nuevo.")
            continue

        dimension = input("Selecciona una dimensión: ")
        if dimension not in dimensiones:
            print("Dimensión no válida. Intenta de nuevo.")
            continue

        if (dimension == "Consistencia") | (dimension == "Exactitud") | (dimension == "Credibilidad"):
            while True:
                validador = input("Ingresa el campo validador: ")
                if validador != "":
                    break
                else:
                    print("El campo 'validador' es obligatorio para la dimensión.")
        else:
            validador = " "

        while True:
            try:
                umbral_minimo = float(input("Ingresa el umbral mínimo (0 a 100): "))
                if 0 <= umbral_minimo <= 100:
                    break
                else:
                    print("Debe ser un valor entre 0 y 100.")
            except ValueError:
                print("Ingresa un número válido.")

        while True:
            try:
                umbral_aceptable = float(input("Ingresa el umbral aceptable (0 a 100): "))
                if 0 <= umbral_aceptable <= 100:
                    if umbral_aceptable < umbral_minimo:
                        print("El umbral aceptable debe ser mayor o igual al umbral mínimo")
                    else:
                        break
                else:
                    print("Debe ser un valor entre 0 y 100.")
            except ValueError:
                print("Ingresa un número válido.")

        configuraciones.append({
            "encabezado": encabezado,
            "dimension": dimension,
            "validador": validador,
            "umbral_minimo": umbral_minimo,
            "umbral_aceptable": umbral_aceptable
        })

    clear_output(wait=True)
    df_nuevo = pd.DataFrame(configuraciones)
    IMPRIMIR_PARAMETRIA(configuraciones)
    GRABA_CONFIGURACION(nombre_archivo, configuraciones)
    IMPRIMIR_PARAMETRIA(configuraciones)
    return configuraciones


# #### función GRABA_CONFIGURACIÓN
# Función que permite grabar la configuración final ya registrada

# In[12]:


def GRABA_CONFIGURACION(nombre_archivo, configuraciones):
    nombre_salida = Path(nombre_archivo + ".conf")  # <- conversión a Path
    df_nuevo = pd.DataFrame(configuraciones)

    if nombre_salida.exists() and respuesta:
        df_existente = pd.read_csv(nombre_salida, sep=';')
        df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
    else:
        df_final = df_nuevo

    df_final.to_csv(nombre_salida, sep=';', index=False)
    clear_output(wait=True)
    print("✅ Configuración guardada correctamente:", nombre_salida)


# ### PARTE 2: EVALUACIÓN DE LA CALIDAD
# Esta sección presenta las funciones principales para la validación de la calidad, se presenta una función por cada dimensión a evaluar, inicialmente se están evaluando únicamente 5 dimensiones, esto debido a que se trata únicamente de un prototipo; sin embargo, en la solución final propuesta debería ampliarse esto; las dimenciones a evaluar son:<br>
# >- Exactitud
# >- Unicidad
# >- Credibilidad
# >- Consistencia
# >- Completitud

# #### Función Exactictitud 
# Esta función mide la dimensión exactitud de un campo, valida si este se encuentra en un rango determinado, para ello, dentro del campo validador se ingresa el rango sobre el que se debe realizar esta validación, se registra un rango mínimo y un rango máximo separado por dos puntos ":" siendo el primer valor el rango mínimo y el segundo valor el rango máximo, funciona tanto para valores numéricos como para valores tipo fecha, se recomiendo que antes de medir la exactitud se mida la consistencia para estar seguros que el campo presenta el formato correcto. <br> Ejemplo del campo validador:<br>
# >- 0:500 (valores entre 0 y 500)
# >- 100:1000 (Valores entre 100 y 1000)
# >- 2025/01/01 : 2025/12/31 (Valores entre el 2025/01/01 y el 2025/12/31
# >- :999999 (Valores menores o iguales a 999999)
# >- 0: (Valores mayores o iguales a 0) 

# In[13]:


def EXACTITUD(df, campo, validador, umbral_minimo, umbral_aceptable):
    # Soporte para campo único o lista de campos (concatenados)
    if isinstance(campo, list):
        serie = df[campo].astype(str).agg(" ".join, axis=1)
    else:
        serie = df[campo].astype(str)

    total_registros = len(serie)
    registros_validos = 0

    # ¿Es un rango de fechas? → formato esperado: 'dd/mm/yyyy : dd/mm/yyyy' o 'yyyy-mm-dd : yyyy-mm-dd'
    if re.search(r"\d{2}/\d{2}/\d{4}", validador) or re.search(r"\d{4}-\d{2}-\d{2}", validador):
        try:
            min_str, max_str = [x.strip() for x in validador.split(":")]
            min_fecha = pd.to_datetime(min_str, dayfirst=True, errors='coerce') if min_str else None
            max_fecha = pd.to_datetime(max_str, dayfirst=True, errors='coerce') if max_str else None
        except Exception as e:
            raise ValueError(f"❌ Error al interpretar rango de fechas: {validador} → {e}")

        for valor in serie:
            fecha = pd.to_datetime(valor, dayfirst=True, errors='coerce')
            if pd.notna(fecha):
                if ((min_fecha is None or fecha >= min_fecha) and
                    (max_fecha is None or fecha <= max_fecha)):
                    registros_validos += 1

    else:
        # Rango numérico → formato esperado: 'min : max'
        try:
            min_str, max_str = [x.strip() for x in validador.split(":")]
            min_val = float(min_str) if min_str else None
            max_val = float(max_str) if max_str else None
        except Exception as e:
            raise ValueError(f"❌ Error al interpretar rango numérico: {validador} → {e}")

        for valor in serie:
            try:
                num = float(valor.replace(",", ""))  # Permitir números con miles separadores
                if ((min_val is None or num >= min_val) and
                    (max_val is None or num <= max_val)):
                    registros_validos += 1
            except Exception:
                pass  # Ignorar no numéricos

    porcentaje = round((registros_validos / total_registros) * 100, 2)

    # Determinar ícono y mensaje
    if porcentaje < umbral_minimo:
        icono = '🔴'
        mensaje = mensaje_rojo
    elif porcentaje < umbral_aceptable:
        icono = '🟡'
        mensaje = mensaje_amarillo
    else:
        icono = '🟢'
        mensaje = mensaje_verde

    return {
        "campo": campo,
        "dimension": "Exactitud",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje,
        "icono": icono,
        "mensaje": mensaje
    }



# #### Función Unicidad
# Esta función mide la dimensión Unicidad referenciada en las normas ISO 8000 (s.f.) y la norma ISO/IEC 25012 (2008), también es mencionada dentro de las normas DAMA DMBok para el gobierno del dato bajo el nombre de No Duplicidad, la función medirá el grado en el que los registros son únicos en el conjunto de datos, y estableciendo si la calidad medida se encuentra por debajo del umbral mínimo, se encuentra dentro del umbral aceptable o está por encima de este.

# In[14]:


def UNICIDAD(df, campo, validador, umbral_minimo, umbral_aceptable):
    if isinstance(campo, str):
        campos = [c.strip() for c in campo.split(",")]
    else:
        campos = campo

    total_registros = len(df)
    registros_unicos = df.drop_duplicates(subset=campos)
    cantidad_unicos = len(registros_unicos)

    porcentaje = round((cantidad_unicos / total_registros) * 100, 2)

    if porcentaje < umbral_minimo:
        icono = "🔴"
        mensaje = mensaje_rojo
    elif porcentaje < umbral_aceptable:
        icono = "🟡"
        mensaje = mensaje_amarillo
    else:
        icono = "🟢"
        mensaje = mensaje_verde

    return {
        "campo": ", ".join(campos),
        "dimension": "Unicidad",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje,
        "icono": icono,
        "mensaje": mensaje
    }


# #### Función Credibilidad
# La credibilidad está enmarcada dentro de las características o dimensiones inherentes al dato según la norma ISO/IEC 25012, esta función permite permite medir que tan confiables son los valores del conjunto de datos con respecto a una lista de datos permitidos, para ello la función evalúa cada uno de los datos y los compara con una lista o validador establecido en la configuración de la calidad, sobre esto establece el porcentaje de calidad del dato y lo compara con los umbrales mínimos y máximos establecidos para el dato y la dimensión, estos valores y su validador enmarcados dentro de lo que se refiere a la configuración del dato dentro de su uso y contexto.

# In[15]:


def CREDIBILIDAD(df, campo, validador, umbral_minimo, umbral_aceptable):
    # 1) Validar validador no vacío
    if not validador:
        raise ValueError(f"⚠️ El validador es obligatorio para el campo '{campo}'")

    # 2) Normalizar lista de columnas
    if isinstance(campo, (list, tuple)):
        campos = list(campo)
    else:
        campos = [campo]

    # 3) Parsear validador a lista de valores permitidos
    if validador.strip().startswith("[") and validador.strip().endswith("]"):
        try:
            valores_permitidos_raw = ast.literal_eval(validador)
            if not isinstance(valores_permitidos_raw, (list, tuple)):
                raise
        except Exception:
            raise ValueError(f"❌ No pude parsear el validador como lista: '{validador}'")
    else:
        valores_permitidos_raw = [v.strip() for v in validador.split(",") if v.strip()]

    # 4) Crear set de strings para comparar
    valores_permitidos = set(str(v) for v in valores_permitidos_raw)

    total = len(df)

    # 5) Función que valida fila por fila
    def fila_valida(row):
        for col in campos:
            x = row[col]
            if pd.isna(x):
                return False
            # Convertir float entero a int para que '1.0' → '1'
            if isinstance(x, float) and x.is_integer():
                x = int(x)
            if str(x).strip() not in valores_permitidos:
                return False
        return True

    # 6) Contar filas válidas
    registros_validos = df.apply(fila_valida, axis=1).sum()

    pct = round((registros_validos / total) * 100, 2)

    # 7) Selección de semáforo y mensaje
    if pct < umbral_minimo:
        icono, mensaje = "🔴", mensaje_rojo
    elif pct < umbral_aceptable:
        icono, mensaje = "🟡", mensaje_amarillo
    else:
        icono, mensaje = "🟢", mensaje_verde

    # 8) Devolver resultado
    return {
        "campo": campos if len(campos) > 1 else campos[0],
        "dimension": "Credibilidad",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": pct,
        "icono": icono,
        "mensaje": mensaje
    }


# #### Función Consistencia
# La consistencia está presente en la norma ISO/IEC 25012 y en la norma ISO 8000 y mide el grado en que los datos están libres de contradicción y con coherentes dentro de un contexto y uso definido, la presente función comprueba que los datos cumplan un patron definido según lo especificado en la configuración, calcula cuantitativamente el porcentaje de calidad según el resultado de la evaluación y lo compara con los umbrales mínimo y máximo establecidos para el dato y la dimensión.

# In[16]:


def CONSISTENCIA(df, campos, validador, umbral_minimo, umbral_aceptable):
    regex = FORMATO_A_REGEX(validador)
    total_registros = len(df)
    registros_validos = 0
    for _, fila in df.iterrows():
        concatenado = ''.join([str(fila[c]).strip() if pd.notna(fila[c]) else '' for c in campos])
        if re.fullmatch(regex, concatenado):
            registros_validos += 1

    porcentaje = round((registros_validos / total_registros) * 100, 2)

    if porcentaje < umbral_minimo:
        icono = '🔴'
        mensaje = mensaje_rojo
    elif porcentaje < umbral_aceptable:
        icono = '🟡'
        mensaje = mensaje_amarillo
    else:
        icono = '🟢'
        mensaje = mensaje_verde

    return {
        "campo": campos,
        "dimension": "Consistencia",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje,
        "icono": icono,
        "mensaje": mensaje
    }


# #### Función Completitud
# La completitud es una dimensión de la calidad considerada inherente al dato, se encuentra enmarcada en la norma ISO/IEC 25012 y en la norma ISO 8000, también es mencionada como una característica del dato y como parte de la evaluación propia del gobierno de datos en las normas DAMA - DMBok, esta dimensión o característica mide el grado en que los valores asociados a un atributo presentan valores. La función evalúa el dato configurado y establece un valor cuantitativo según la presencia de los valores en el conjunto de datos, comparándolo posteriormente con los umbrales mínimo y aceptable configurados para el campo y dimensión.

# In[17]:


def COMPLETITUD(df, campo, validador, umbral_minimo, umbral_aceptable):
    if isinstance(campo, str):
        campo = [campo]

    total_registros = len(df)

    registros_con_dato = df[campo].apply(
        lambda fila: all(pd.notna(x) and str(x).strip() != '' for x in fila),
        axis=1
    ).sum()

    porcentaje_completitud = round((registros_con_dato / total_registros) * 100, 2)

    if porcentaje_completitud < umbral_minimo:
        icono = "🔴"
        mensaje = mensaje_rojo
    elif porcentaje_completitud < umbral_aceptable:
        icono = "🟡"
        mensaje = mensaje_amarillo
    else:
        icono = "🟢"
        mensaje = mensaje_verde

    return {
        "campo": campo,
        "dimension": "Completitud",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje_completitud,
        "icono": icono,
        "mensaje": mensaje
    }



# #### Calcula el promedio de la calidad por dimensión
# Esta función toma todas las dimensiones de calidad evaluadas en el conjunto de datos y calcula el promedio obtenido en la evaluación por cada una de estas, con esto se pretende dar un valor cuantitativo de la calidad por cada una de las dimensiones evaluadas a nivel de conjunto de datos y no del campo propiamente dicho

# In[18]:


def PROMEDIO_CALIDAD_DIMENSIONES(resultado):
    from collections import defaultdict

    sumatorias = defaultdict(float)
    cantidades = defaultdict(int)

    for r in resultado:
        dimension = r['dimension']
        sumatorias[dimension] += r['calidad']
        cantidades[dimension] += 1

    promedios = {}
    for dimension in sumatorias:
        promedios[dimension] = round(sumatorias[dimension] / cantidades[dimension], 2)

    return promedios


# #### Calcula el promedio de la calidad por campo
# Esta función toma todas los campos del dataset evaluado y calcula el promedio obtenido en la evaluación por cada uno de estos, con esto se pretende dar un valor cuantitativo de la calidad por cada uno de los campos evaluadas.

# In[19]:


def PROMEDIO_CALIDAD_CAMPOS(resultado, decimales=2):
    from collections import defaultdict

    sumatorias = defaultdict(float)
    cantidades = defaultdict(int)

    for r in resultado:
        # Convertir el campo a string para usarlo como clave
        campo = ", ".join(r['campo']) if isinstance(r['campo'], list) else str(r['campo'])
        sumatorias[campo] += r['calidad']
        cantidades[campo] += 1

    promedios = {campo: round(sumatorias[campo] / cantidades[campo], decimales) for campo in sumatorias}
    return promedios


# #### Calcula el promedio de la calidad por semáfono
# Esta función toma todas las opciones de semaforización del dataset evaluado y calcula el promedio obtenido en la evaluación por cada uno de estos, con esto se pretende dar un valor cuantitativo de la calidad por cada uno de los posibles valores.

# In[20]:


def PROMEDIO_CALIDAD_RESULTADO(resultado, decimales=2):
    from collections import defaultdict

    sumatorias = defaultdict(float)
    cantidades = defaultdict(int)

    for r in resultado:
        mensaje = r['icono'] + ' ' + r['mensaje']
        sumatorias[mensaje] += r['calidad']
        cantidades[mensaje] += 1

    promedios = {}
    for mensaje in sumatorias:
        promedios[mensaje] = cantidades[mensaje] #round(sumatorias[mensaje] / cantidades[mensaje], 2)

    return promedios


# #### Función CARGA_PARAMETRIA
# Esta función es el punto de partida del evaluador, carga la parametría registrada en el paso 1 para en base a esta iniciar la evaluación del conjunto de datos

# In[21]:


def CARGA_PARAMETRIA():
    nombre_archivo = input("\nIngrese el nombre del archivo de datos a evaluar (sin extensión): ")
    archivo_data      = nombre_archivo + ".csv"
    archivo_parametro = nombre_archivo + ".conf"

    df_data = LEER_CSV(archivo_data, ";")
    df_parametro = LEER_CSV(archivo_parametro, ";")

    configuraciones = []
    df_parametro['encabezado'] = df_parametro['encabezado'].apply(ast.literal_eval)
    configuraciones = df_parametro.to_dict(orient="records")

    encabezados = list(df_data.columns)

    clear_output(wait=True)
    IMPRIMIR_TITULO("CONJUNTO DE DATOS Y NOMBRE DE CAMPOS A EVALUAR")
    print("")
    print(archivo_data)
    print(encabezados)
    print("")
    IMPRIMIR_PARAMETRIA(configuraciones)

    return nombre_archivo, configuraciones, df_data, df_parametro


# #### función EVALUADOR
# Función principal para la evaluación de la calidad.

# In[22]:


funciones_dimensiones = {
    "Exactitud": EXACTITUD,
    "Completitud": COMPLETITUD,
    "Consistencia": CONSISTENCIA,
    "Credibilidad": CREDIBILIDAD,
    "Unicidad": UNICIDAD
}

def EVALUADOR(configuraciones, df_data):
    resultado = []
    vuelta = 1
    longitud = len(configuraciones)
    display("⏳ Procesando...")
    print(f"Avance ... {0:.2f}%")
    for config in configuraciones:
        nombre_dim = config['dimension']
        avance = round(100 * vuelta / longitud,2)
        print(f"Avance ... {avance:.2f}%")
        funcion = funciones_dimensiones.get(nombre_dim)
        vuelta = vuelta + 1
        if funcion:
            resultado_dim = funcion(
                df_data,
                config['encabezado'],
                config['validador'],
                config['umbral_minimo'],
                config['umbral_aceptable']
            )
            resultado.append(resultado_dim)

    clear_output(wait=True)
    IMPRIMIR_RESULTADO_FINAL(resultado)
    return resultado


# ## SECCIÓN FASE 3 DE LA METODOLOGÍA - MUESTRA DE RESULTADOS DE LA EVALUACIÓN (SALIDA)

# #### Función IMPRIMIR_RESULTADO_FINAL
# Esta función realiza la impresión en pantalla los 4 tableros resultado de la evaluación de la calidad de un conjunto de datos

# In[23]:


def IMPRIMIR_RESULTADO_FINAL(resultado):
    IMPRIMIR_TITULO("RESULTADOS EVALUACIÓN DE LA CALIDAD")
    print("-----------------------------------")
    print("")
    IMPRIMIR_TITULO("MEDICIÓN DE LA CALIDAD POR CAMPO Y DIMENSIÓN")
    print("")
    print(f"{' S   ':<2} {'Campo':<27} {'Dimension':<20} {'Calidad':<14} {'Umbral Min':<15} {'Umbral Acep':<14} {'Resultado':<70}")
    print("-" * 170)
    for r in resultado:
        IMPRIMIR_RESULTADO(r["icono"], 
                           r["campo"], 
                           r["dimension"], 
                           r["calidad"],  
                           r["umbral_minimo"], 
                           r["umbral_aceptable"],
                           r["mensaje"])
    print("")

    IMPRIMIR_TITULO("MEDICIÓN DE LA CALIDAD POR DIMENSIÓN")
    print("")
    print(f"{'Dimension':<25} {'Calidad':>14}")
    print("-" * 40)
    promedios_dimension = PROMEDIO_CALIDAD_DIMENSIONES(resultado)
    for dimension, promedios_dimension in promedios_dimension.items():
        print(f"{dimension:<25} | {promedios_dimension:>10.2f}%")

    print("")

    IMPRIMIR_TITULO("MEDICION DE LA CALIDAD POR CAMPO")
    print("")
    print(f"{'Campo':<27} {'Calidad':>14}")
    print("-" * 42)
    promedios_campos = PROMEDIO_CALIDAD_CAMPOS(resultado)
    for campo, promedio_campos in promedios_campos.items():
        print(f"{campo[:25]:<27} | {promedio_campos:>10.2f}%")

    print("")

    IMPRIMIR_TITULO("MEDICIÓN DE LA CALIDAD POR SEMÁFORO")
    print("")
    print(f"{'Semáforo':<73} {'Resultado':>15}")
    print("-" * 96)
    total = len(resultado)
    cantidad_icono = PROMEDIO_CALIDAD_RESULTADO(resultado)
    for mensaje, cantidad_icono in cantidad_icono.items():
        porcentaje = 100 * cantidad_icono / total
        print(f"{mensaje:<73} | {cantidad_icono:>3} / {total:<3} - {porcentaje:>5.2f}%")


# #### Función MAPA_CALOR
# Esta función dibuja en pantalla un mapa de calor con los resultados obtenidos en la evaluación de la calidad

# In[6]:


def MAPA_CALOR(resultado):
    df_resultado = pd.DataFrame(resultado)

    # Convertir listas a texto plano
    df_resultado["campo"] = df_resultado["campo"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )

    # Dividir etiquetas largas en 2 líneas
    def dividir_linea(texto, max_len=20):
        if len(texto) > max_len:
            corte = texto[:max_len].rfind(" ")
            if corte == -1:
                corte = max_len
            return texto[:corte] + '\n' + texto[corte:]
        return texto

    # Aplicar a una nueva columna (para preservar campo original si se desea)
    df_resultado["campo_etiqueta"] = df_resultado["campo"].apply(dividir_linea)

    # Usar campo dividido como índice en el heatmap
    heatmap_data = df_resultado.pivot_table(
        index="campo_etiqueta",
        columns="dimension",
        values="calidad",
        aggfunc="mean"
    )

    plt.figure(figsize=(10, 6))
    sns.heatmap(
        heatmap_data,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        linewidths=0.5,
        cbar=True,
        square=False,
        annot_kws={"fontsize": 9},
        vmin=0,
        vmax=100
    )

    plt.title("Mapa de Calor - Calidad por Campo y Dimensión", fontsize=14, weight="bold")
    plt.xlabel("Dimensión", fontsize=10)
    plt.ylabel("Campo", fontsize=10)
    plt.xticks(rotation=0)
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.show()


# #### Función DIAGRAMA_BURBUJAS
# Función que dibuja en pantalla un diagrama de burbujas con el resultado de la evaluación

# In[5]:


def DIAGRAMA_BURBUJAS(resultado):
    df_resultado = pd.DataFrame(resultado)

    # Convertir listas a texto
    df_resultado['campo_str'] = df_resultado['campo'].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )

    # Función para dividir etiquetas largas en 2 líneas
    def dividir_linea(texto, max_len=20):
        if len(texto) > max_len:
            corte = texto[:max_len].rfind(" ")
            if corte == -1:
                corte = max_len
            return texto[:corte] + '\n' + texto[corte:]
        return texto

    df_resultado['campo_str'] = df_resultado['campo_str'].apply(dividir_linea)

    # Crear gráfico de burbujas
    plt.figure(figsize=(8, 4))
    sns.scatterplot(
        data=df_resultado,
        x="dimension",
        y="campo_str",
        size="calidad",
        hue="calidad",
        palette="viridis",
        sizes=(50, 500),
        legend="brief"
    )

    plt.title("Gráfico de Burbujas - Calidad por Campo y Dimensión", fontsize=14, weight="bold")
    plt.xlabel("Dimensión", fontsize=10)
    plt.ylabel("Campo", fontsize=10)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()


# #### Función DIAGRAMA_RADAR
# Función que dibuja en pantalla un diagrama de radar con el resultado de la evaluación

# In[26]:


def DIAGRAMA_RADAR(resultado):
    df_resultado = pd.DataFrame(resultado)
    promedios = df_resultado.groupby("dimension")["calidad"].mean()

    labels = list(promedios.index)
    num_vars = len(labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    values = promedios.tolist()
    values += values[:1]  
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(5, 5), subplot_kw=dict(polar=True))
    ax.plot(angles, values, color='teal', linewidth=2)
    ax.fill(angles, values, color='teal', alpha=0.25)
    ax.set_thetagrids(np.degrees(angles[:-1]), labels)
    ax.set_title("Promedio de Calidad por Dimensión", fontsize=14, weight="bold")
    ax.set_ylim(0, 100)
    plt.show()


# #### Función DIAGRAMA_BARRAS_DIMENSION
# Función que dibuja en pantalla un diagrama de barras por las dimensiones evaluadas según el promedio simple de cada una de llas

# In[1]:


def DIAGRAMA_BARRAS_DIMENSION(resultado):
    df_resultado = pd.DataFrame(resultado)
    promedios = df_resultado.groupby("dimension")["calidad"].mean().reset_index()
    promedios = promedios.sort_values("calidad", ascending=False)

    fig, ax = plt.subplots(figsize=(7, 5))
    colores = plt.cm.Set2.colors
    barras = ax.bar(
        promedios["dimension"],
        promedios["calidad"],
        color=colores[:len(promedios)]
    )

    for barra in barras:
        altura = barra.get_height()-10
        ax.annotate(f'{altura + 10:.2f}%',
                    xy=(barra.get_x() + barra.get_width() / 2, altura),
                    xytext=(0, 3),  
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=10)

    ax.set_title("Distribución de la calidad por dimensión", fontsize=12, weight="bold")
    ax.set_ylabel("Calidad promedio (%)", fontsize=12)
    ax.set_xlabel("Dimensión", fontsize=12)
    ax.set_ylim(0, 100)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()


# #### Función DIAGRAMA_DONA
# Función que realiza un diagrama de Dona con la distribución porcentual de la calidad por cada una de las dimensiones evaluadas

# In[28]:


def DIAGRAMA_DONA(resultado):
    df_resultado = pd.DataFrame(resultado)

    promedios = df_resultado.groupby("dimension")["calidad"].mean().reset_index()

    fig, ax = plt.subplots(figsize=(5, 5))

    colores = plt.cm.Set2.colors 
    wedges, texts, autotexts = ax.pie(
        promedios["calidad"],
        labels=promedios["dimension"],
        autopct='%1.1f%%',
        startangle=90,
        pctdistance=0.85,
        colors=colores,
        textprops={'fontsize': 12}
    )

    centro = plt.Circle((0,0), 0.60, fc='white')
    fig.gca().add_artist(centro)

    ax.set_title("Distribución de la calidad por dimensión", fontsize=14, weight="bold")

    plt.tight_layout()
    plt.show()


# #### Función DIAGRAMA_PIE
# Función que dibuja un diagrama de pie en pantalla en base a la distribución de la calidad por dimensión

# In[29]:


def DIAGRAMA_PIE(resultado):
    df_resultado = pd.DataFrame(resultado)

    promedios = df_resultado.groupby("dimension")["calidad"].mean().reset_index()

    fig, ax = plt.subplots(figsize=(5, 5))

    colores = plt.cm.Set2.colors
    wedges, texts, autotexts = ax.pie(
        promedios["calidad"],
        labels=promedios["dimension"],
        autopct='%1.1f%%',
        startangle=90,
        colors=colores,
        textprops={'fontsize': 12}
    )

    ax.set_title("Distribución de la calidad por dimensión", fontsize=14, weight="bold")

    plt.tight_layout()
    plt.show()


# #### Funión DIAGRAMA_BARRA_CAMPOS
# Función que realiza un diagrama de barras según el promedio de calidad obtenido por cada uno de los campos evaluados

# In[4]:


def DIAGRAMA_BARRA_CAMPOS(resultado):
    df_resultado = pd.DataFrame(resultado)

    # Unir listas de campos en una sola cadena
    df_resultado["campo"] = df_resultado["campo"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )

    # Dividir etiquetas largas en dos líneas si superan 20 caracteres
    def dividir_linea(texto, max_len=20):
        if len(texto) > max_len:
            corte = texto[:max_len].rfind(" ")
            if corte == -1:
                corte = max_len
            return texto[:corte] + '\n' + texto[corte:]
        return texto

    df_resultado["campo"] = df_resultado["campo"].apply(dividir_linea)

    # Calcular promedios
    promedios = df_resultado.groupby("campo")["calidad"].mean().reset_index()

    # Colores (todos iguales)
    azul_unico = ["#1f77b4"] * len(promedios)

    plt.figure(figsize=(10, 6))
    barras = sns.barplot(
        data=promedios, 
        y="campo", 
        x="calidad", 
        hue="campo",           # ← Necesario para usar palette sin warning
        palette=azul_unico,
        legend=False
    )

    # Añadir etiquetas de porcentaje
    for i, row in promedios.iterrows():
        barras.text(
            row.calidad + 1,   # x: ligeramente a la derecha del final de la barra
            i,                 # y: posición de la barra
            f"{row.calidad:.2f}%", 
            color='black', va='center', fontsize=9
        )

    plt.title("Promedio de Calidad por Campo", fontsize=14, weight="bold")
    plt.xlabel("Calidad (%)", fontsize=10)
    plt.ylabel("Campo", fontsize=10)
    plt.xlim(0, 110)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()


# #### Función DIAGRAMA_KPI
# Función que realiza un diagrama en modo velocímetro con el KPI global de la calidad del conjunto de datos

# In[31]:


def DIAGRAMA_KPI(resultado):
    df_resultado = pd.DataFrame(resultado)
    df_expandido = df_resultado.explode("campo").reset_index(drop=True)
    indice_global = round(df_expandido["calidad"].mean(),2)

    fig, ax = plt.subplots(figsize=(6, 3))

    theta = np.linspace(np.pi, 0, 100)
    x = 0.90 * np.cos(theta)
    y = 0.90 * np.sin(theta)
    ax.plot(x, y, color='lightgray', linewidth=40, solid_capstyle='projecting')

    # Aguja
    angulo = np.pi * (1 - indice_global / 100)  # convierte 0–100 a pi–0
    x_aguja = np.cos(angulo)
    y_aguja = np.sin(angulo)
    ax.plot([0, x_aguja], [0, y_aguja], color='black', linewidth=3)
    ax.plot(0, 0, 'o', color='black')  # centro

    # Texto central
    ax.text(0, -0.2, f"{indice_global:.1f}%", fontsize=18, weight='bold', ha='center', va='center')

    # Título
    ax.set_title("KPI: Índice global de calidad del dato", fontsize=12, weight="bold", pad=10)

    for valor in np.linspace(0, 100, 11):  # de 0 a 100 en pasos de 10%
        angulo = np.pi * (1 - valor / 100)
        x_start = 0.75 * np.cos(angulo)
        y_start = 0.75 * np.sin(angulo)
        x_end = 0.85 * np.cos(angulo)
        y_end = 0.85 * np.sin(angulo)
        ax.plot([x_start, x_end], [y_start, y_end], color='black', linewidth=0.5)

    ax.set_xlim(-1.1, 1.1)
    ax.set_ylim(-0.3, 1.1)

    # Eliminar ejes y mantener proporción
    ax.set_aspect('equal')
    ax.axis('off')
    plt.tight_layout()
    plt.show()


# #### Generador de PDF original
# Esta versión ya no se genera, fue reemplazada por la función que se encuentra en la parte inferior

# In[32]:


def GENERAR_PDF_COMPLETO_ORIGINAL(df_resultado, nombre_archivo):
    with PdfPages(nombre_archivo + ".pdf") as pdf:
        fig = plt.figure(figsize=(14, 10))

        # SECCION 1 - Tabla con semáforo
        ax1 = plt.subplot2grid((3, 4), (0, 0), colspan=4)
        ax1.axis('off')

        columnas = list(df_resultado.columns)
        col_index = columnas.index("icono")
        mensaje_index = columnas.index("mensaje")

        tabla = ax1.table(
            cellText=df_resultado.values,
            colLabels=columnas,
            cellLoc='center',
            loc='center'
        )
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(9)
        tabla.scale(1.2, 2)

        col_widths = {0: 0.2, 2: 0.08, 3: 0.08, 4: 0.08, 5: 0.08, mensaje_index: 0.48}
        for col, width in col_widths.items():
            tabla.auto_set_column_width(col)
            for row in range(len(df_resultado) + 1):
                cell = tabla[row, col]
                cell.set_width(width)
                if col == mensaje_index:
                    cell.get_text().set_ha('left')

        for i in range(len(df_resultado)):
            simbolo, color = OBTENER_SEMAFORO(
                df_resultado.iloc[i]['calidad'],
                df_resultado.iloc[i]['umbral_minimo'],
                df_resultado.iloc[i]['umbral_aceptable']
            )
            tabla[i + 1, col_index].get_text().set_text(simbolo)
            tabla[i + 1, col_index].get_text().set_color(color)

        ax1.set_title("Resultados de Calidad", fontsize=12, weight='bold')

        # SECCIÓN 2 - Diagrama de Radar
        ax2 = plt.subplot2grid((3, 4), (1, 0), colspan=2, rowspan=2, polar=True)
        promedio = df_resultado.groupby('dimension')['calidad'].mean()
        categorias = promedio.index.tolist()
        valores = promedio.values.tolist() + [promedio.values[0]]
        angulos = [n / float(len(categorias)) * 2 * pi for n in range(len(categorias))] + [0]
        ax2.plot(angulos, valores, linewidth=2, linestyle='solid')
        ax2.fill(angulos, valores, alpha=0.3)
        ax2.set_theta_offset(pi / 2)
        ax2.set_theta_direction(-1)
        ax2.set_xticks(angulos[:-1])
        ax2.set_xticklabels(categorias)
        ax2.set_yticks([20, 40, 60, 80, 100])
        ax2.set_ylim(0, 100)
        ax2.set_title("Gráfico Radar", fontsize=10)

        # SECCIÓN 3 - Diagrama de Dona
        ax3 = plt.subplot2grid((3, 4), (1, 2), colspan=2)
        wedges, texts, autotexts = ax3.pie(
            promedio, labels=promedio.index, autopct='%1.1f%%',
            startangle=140, pctdistance=0.85
        )
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        fig.gca().add_artist(centre_circle)
        ax3.axis('equal')
        ax3.set_title("Gráfico Dona", fontsize=10)

        # SECCIÓN 4 - Diagrama de Burbuja
        ax4 = plt.subplot2grid((3, 4), (2, 2), colspan=2)
        burbuja = df_resultado.copy()
        burbuja["campo"] = burbuja["campo"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        x = range(len(burbuja))
        y = burbuja["calidad"]
        sizes = burbuja["calidad"] * 5
        colors = burbuja["calidad"]
        ax4.scatter(x, y, s=sizes, c=colors, cmap="coolwarm", alpha=0.6, edgecolors="black")
        ax4.set_xticks(x)
        ax4.set_xticklabels(burbuja["campo"], rotation=45, ha='right')
        ax4.set_ylim(0, 110)
        ax4.set_ylabel("Calidad (%)")
        ax4.set_title("Gráfico de Burbujas", fontsize=10)
        ax4.grid(True, linestyle='--', alpha=0.5)

        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()

    print("✅ PDF generado correctamente")


# #### Genera Dashboard PDF

# In[33]:


def GENERAR_PDF_COMPLETO(resultado, nombre_archivo):
    matplotlib.rcParams['font.family'] = 'DejaVu Sans'
    df_resultado = pd.DataFrame(resultado)
    nombre_pdf = nombre_archivo + ".pdf"
    with PdfPages(nombre_archivo + ".pdf") as pdf:
        fig = plt.figure(figsize=(16, 10))
        fig.subplots_adjust(top=0.93)
        #fig.suptitle("Dashboard de Calidad del Dato", fontsize=16, weight='bold', y=0.95)
        # === Fila 1: Tabla semáforo ===
        ax1 = plt.subplot2grid((3, 4), (0, 0), colspan=4)
        ax1.axis('off')

        columnas = list(df_resultado.columns)
        col_index = columnas.index("icono")
        mensaje_index = columnas.index("mensaje")

        tabla = ax1.table(
            cellText=df_resultado.values,
            colLabels=columnas,
            cellLoc='center',
            loc='center'
        )
        tabla.auto_set_font_size(False)
        tabla.set_fontsize(9)
        tabla.scale(1.2, 2)

        col_widths = {0: 0.2, 2: 0.08, 3: 0.08, 4: 0.08, 5: 0.08, mensaje_index: 0.48}
        for col, width in col_widths.items():
            tabla.auto_set_column_width(col)
            for row in range(len(df_resultado) + 1):
                cell = tabla[row, col]
                cell.set_width(width)
                if col == mensaje_index:
                    cell.get_text().set_ha('left')

        for i in range(len(df_resultado)):
            simbolo, color = OBTENER_SEMAFORO(
                df_resultado.iloc[i]['calidad'],
                df_resultado.iloc[i]['umbral_minimo'],
                df_resultado.iloc[i]['umbral_aceptable']
            )
            tabla[i + 1, col_index].get_text().set_text(simbolo)
            tabla[i + 1, col_index].get_text().set_color(color)

        ax1.set_title("Resultados de Calidad", fontsize=12, weight='bold', pad=40)

        # === Fila 2 Izquierda: Velocímetro ===
        ax2 = plt.subplot2grid((3, 4), (1, 0), colspan=2)
        ax2.set_aspect('equal')
        promedio_general = round(df_resultado["calidad"].mean(),2)
        theta = np.linspace(np.pi, 0, 100)
        x = 0.9 * np.cos(theta)
        y = 0.9 * np.sin(theta)
        #ax2.plot(x, y, color='lightgray', linewidth=30, solid_capstyle='round')
        ax2.plot(x, y, color='lightgray', linewidth=40, solid_capstyle='projecting')
        angulo = np.pi * (1 - promedio_general / 100)
        x_aguja = 0.9 * np.cos(angulo)
        y_aguja = 0.9 * np.sin(angulo)
        ax2.plot([0, x_aguja], [0, y_aguja], color='black', linewidth=3)
        ax2.plot(0, 0, 'o', color='black')
        ax2.text(0, -0.2, f"{promedio_general:.1f}%", fontsize=18, weight='bold', ha='center')
        for val in np.linspace(0, 100, 11):
            ang = np.pi * (1 - val / 100)
            x0 = 0.85 * np.cos(ang)
            y0 = 0.85 * np.sin(ang)
            x1 = 0.95 * np.cos(ang)
            y1 = 0.95 * np.sin(ang)
            ax2.plot([x0, x1], [y0, y1], color='black', linewidth=1)
        ax2.set_xlim(-1.1, 1.1)
        ax2.set_ylim(-0.3, 1.1)
        ax2.axis('off')
        ax2.set_title("KPI: Índice Global de Calidad", fontsize=10)

        # === Fila 2 Derecha: Radar ===
        ax3 = plt.subplot2grid((3, 4), (1, 2), colspan=2, polar=True)
        promedio = df_resultado.groupby('dimension')['calidad'].mean()
        categorias = promedio.index.tolist()
        valores = promedio.values.tolist() + [promedio.values[0]]
        angulos = [n / float(len(categorias)) * 2 * pi for n in range(len(categorias))] + [0]
        ax3.plot(angulos, valores, linewidth=2, linestyle='solid')
        ax3.fill(angulos, valores, alpha=0.3)
        ax3.set_theta_offset(pi / 2)
        ax3.set_theta_direction(-1)
        ax3.set_xticks(angulos[:-1])
        ax3.set_xticklabels(categorias)
        ax3.set_yticks([20, 40, 60, 80, 100])
        ax3.set_ylim(0, 100)
        ax3.set_title("Gráfico Radar por Dimensión", fontsize=10)

        # === Fila 3 Izquierda: Barras por dimensión ===
        ax4 = plt.subplot2grid((3, 4), (2, 0), colspan=2)
        ax4.barh(promedio.index, promedio.values, color='teal')
        ax4.set_xlim(0, 100)
        ax4.set_title("Calidad Promedio por Dimensión", fontsize=10)
        ax4.set_xlabel("Calidad (%)")

        # === Fila 3 Derecha: Campos críticos ===
        ax5 = plt.subplot2grid((3, 4), (2, 2), colspan=2)
        campos_criticos = df_resultado[df_resultado["calidad"] < df_resultado["umbral_minimo"]].copy()
        if not campos_criticos.empty:
            campos_criticos["campo"] = campos_criticos["campo"].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
            x = range(len(campos_criticos))
            y = campos_criticos["calidad"]
            sizes = campos_criticos["calidad"] * 5
            colors = campos_criticos["calidad"]
            ax5.scatter(x, y, s=sizes, c=colors, cmap="Reds", alpha=0.6, edgecolors="black")
            ax5.set_xticks(x)
            ax5.set_xticklabels(campos_criticos["campo"], rotation=45, ha='right')
            ax5.set_ylim(0, 110)
            ax5.set_ylabel("Calidad (%)")
            ax5.set_title("Campos con Calidad Crítica", fontsize=10)
            ax5.grid(True, linestyle='--', alpha=0.5)
        else:
            ax5.text(0.5, 0.5, "No se encontraron campos críticos", ha='center', va='center')
            ax5.axis('off')

        plt.tight_layout()
        pdf.savefig(fig)
        plt.close()
    print("✅ PDF generado correctamente")


# #### Genera de Dossier

# In[34]:


def DOSSIER(df_data, nombre_archivo):
    nombre_dosier = nombre_archivo + ".html"
    informe = sv.analyze((df_data, "Resultado")) 
    informe.show_html(nombre_dosier)
    clear_output(wait=True)
    print("✅ Dossier generado correctamente:", nombre_dosier)

