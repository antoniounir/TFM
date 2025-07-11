#!/usr/bin/env python
# coding: utf-8

# # TFM - Funciones para la evaluación del la calidad - FuncionesTFM.Py

# ### Datos del autor
# Notebook elaborado por: **César Fernando Balaguer García** y **Antonio Luís Almira Martínez** en el marco de la tesis de Máster Universitario en **Análisis y Visualización de Datos Masivos / Visual Analytic y Big Data** (2025) <br>
# Asesora de tesis: **María Belén Benalcázar Tovar**. <br>
# Título: **Marco Metodológico que permita medir la calidad de los datos de un conjunto de datos en formatos planos**.

# Este script de python tiene todas las funciones necesarias para realizar la validación de los datos de un conjunto de datos, existe una función por cada dimensión de calidad

# In[1]:


import pandas as pd
import re
import ast

from IPython.display import display, clear_output


# ## Sección de definición de funciones
# En esta sección se define una función por cada dimensión de calidad a evaluar, si se desean más funciones se deberán incluir en la colección inicial creada líneas arriba, estas funciones devolveran un valor porcentual la cual indicará cuantos datos cumplen con la dimensión evaluada para el campo que se está evaluando, este último dato lo recibirá como parámetro, al igual que los umbrales mínimo y aceptable.

# ### Función Regex
# Esta función permite cambiar el campo validador registrado por el usuario a un formato que Python pueda entender, se utiliza para algunas de las validaciones de las dimensiones de calidad

# In[2]:


import re

def formato_a_regex(validador):
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

    # ————— Numéricos con #,9, comas y decimales (opcionales o fijos) —————
    # Ejemplos: "9", "##9", "#,##9", "9.99", "##9[.99]", "#,##9.99"
    if re.match(r'^[#9,]+(?:\.\d+)?(?:\[\.\d+\])?$', validador):
        base = validador
        decimal_opcional = False
        dec_fijo = 0

        # Parte decimal entre corchetes → opcional
        if '[' in base:
            base, resto = base.split('[', 1)
            decimal_opcional = True
            dec_fijo = resto.strip(']').count('9')
        # Parte decimal obligatoria con punto
        elif '.' in base:
            base, dec = base.split('.', 1)
            dec_fijo = len(dec)

        # Contar dígitos obligatorios y opcionales
        obligatorios = base.count('9')
        opcionales   = base.count('#')

        # Regex parte entera (con o sin separador de miles)
        if ',' in base:
            entero_regex = r'(?:\d{1,3}(?:,\d{3})*)'
        else:
            if obligatorios > 0:
                entero_regex = rf'\d{{{obligatorios},{obligatorios + opcionales}}}'
            else:
                # Solo opcionales → al menos 1 dígito
                entero_regex = rf'\d{{1,{opcionales}}}'

        # Regex parte decimal
        if dec_fijo > 0:
            parte_dec = rf'\.\d{{{dec_fijo}}}'
            if decimal_opcional:
                parte_dec = rf'(?:{parte_dec})?'
        else:
            parte_dec = ''

        return rf'^{entero_regex}{parte_dec}$'

    # ————— Texto de longitud variable —————
    if re.match(r'^X\d+$', validador):
        largo = int(validador[1:])
        return rf'^.{{0,{largo}}}$'

    # ————— Ningún caso válido —————
    raise ValueError(f"❌ Formato de validador no reconocido: '{validador}'")


# ### Función Exactictitud 
# Esta función mide la dimensión exactitud de un campo, valida si este se encuentra en un rango determinado, para ello, dentro del campo validador se ingresa el rango sobre el que se debe realizar esta validación, se registra un rango mínimo y un rango máximo separado por dos puntos ":" siendo el primer valor el rango mínimo y el segundo valor el rango máximo, funciona tanto para valores numéricos como para valores tipo fecha, se recomiendo que antes de medir la exactitud se mida la consistencia para estar seguros que el campo presenta el formato correcto. <br> Ejemplo del campo validador:<br>
# >- 0:500 (valores entre 0 y 500)
# >- 100:1000 (Valores entre 100 y 1000)
# >- 2025/01/01 : 2025/12/31 (Valores entre el 2025/01/01 y el 2025/12/31
# >- :999999 (Valores menores o iguales a 999999)
# >- 0: (Valores mayores o iguales a 0) 

# In[3]:


def exactitud(df, campo, validador, umbral_minimo, umbral_aceptable):
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
        mensaje = "Debajo del umbral mínimo"
    elif porcentaje < umbral_aceptable:
        icono = '🟡'
        mensaje = "Debajo del umbral aceptable"
    else:
        icono = '🟢'
        mensaje = "Cumple el umbral aceptable"

    return {
        "campo": campo,
        "dimension": "Exactitud",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje,
        "icono": icono,
        "mensaje": mensaje
    }



# ### Función Unicidad
# Esta función mide la dimensión Unicidad referenciada en las normas ISO 8000 (s.f.) y la norma ISO/IEC 25012 (2008), también es mencionada dentro de las normas DAMA DMBok para el gobierno del dato bajo el nombre de No Duplicidad, la función medirá el grado en el que los registros son únicos en el conjunto de datos, y estableciendo si la calidad medida se encuentra por debajo del umbral mínimo, se encuentra dentro del umbral aceptable o está por encima de este.

# In[4]:


def unicidad(df, campo, validador, umbral_minimo, umbral_aceptable):
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
        mensaje = "Debajo del umbral mínimo"
    elif porcentaje < umbral_aceptable:
        icono = "🟡"
        mensaje = "Debajo del umbral aceptable"
    else:
        icono = "🟢"
        mensaje = "Cumple el umbral aceptable"

    return {
        "campo": ", ".join(campos),
        "dimension": "Unicidad",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje,
        "icono": icono,
        "mensaje": mensaje
    }


# ### Función Credibilidad
# La credibilidad está enmarcada dentro de las características o dimensiones inherentes al dato según la norma ISO/IEC 25012, esta función permite permite medir que tan confiables son los valores del conjunto de datos con respecto a una lista de datos permitidos, para ello la función evalúa cada uno de los datos y los compara con una lista o validador establecido en la configuración de la calidad, sobre esto establece el porcentaje de calidad del dato y lo compara con los umbrales mínimos y máximos establecidos para el dato y la dimensión, estos valores y su validador enmarcados dentro de lo que se refiere a la configuración del dato dentro de su uso y contexto.

# In[5]:


def credibilidad(df, campo, validador, umbral_minimo, umbral_aceptable):
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
        icono, mensaje = "🔴", "Debajo del umbral mínimo"
    elif pct < umbral_aceptable:
        icono, mensaje = "🟡", "Debajo del umbral aceptable"
    else:
        icono, mensaje = "🟢", "Cumple el umbral aceptable"

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


# ### Función Consistencia
# La consistencia está presente en la norma ISO/IEC 25012 y en la norma ISO 8000 y mide el grado en que los datos están libres de contradicción y con coherentes dentro de un contexto y uso definido, la presente función comprueba que los datos cumplan un patron definido según lo especificado en la configuración, calcula cuantitativamente el porcentaje de calidad según el resultado de la evaluación y lo compara con los umbrales mínimo y máximo establecidos para el dato y la dimensión.

# In[6]:


def consistencia(df, campos, validador, umbral_minimo, umbral_aceptable):
    regex = formato_a_regex(validador)
    total_registros = len(df)
    registros_validos = 0

    for _, fila in df.iterrows():
        concatenado = ''.join([str(fila[c]).strip() if pd.notna(fila[c]) else '' for c in campos])
        if re.fullmatch(regex, concatenado):
            registros_validos += 1

    porcentaje = round((registros_validos / total_registros) * 100, 2)

    if porcentaje < umbral_minimo:
        icono = '🔴'
        mensaje = "Debajo del umbral mínimo"
    elif porcentaje < umbral_aceptable:
        icono = '🟡'
        mensaje = "Debajo del umbral aceptable"
    else:
        icono = '🟢'
        mensaje = "Cumple el umbral aceptable"

    return {
        "campo": campos,
        "dimension": "Consistencia",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje,
        "icono": icono,
        "mensaje": mensaje
    }


# ### Función Completitud
# La completitud es una dimensión de la calidad considerada inherente al dato, se encuentra enmarcada en la norma ISO/IEC 25012 y en la norma ISO 8000, también es mencionada como una característica del dato y como parte de la evaluación propia del gobierno de datos en las normas DAMA - DMBok, esta dimensión o característica mide el grado en que los valores asociados a un atributo presentan valores. La función evalúa el dato configurado y establece un valor cuantitativo según la presencia de los valores en el conjunto de datos, comparándolo posteriormente con los umbrales mínimo y aceptable configurados para el campo y dimensión.

# In[7]:


def completitud(df, campo, validador, umbral_minimo, umbral_aceptable):
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
        mensaje = "debajo del umbral mínimo"
    elif porcentaje_completitud < umbral_aceptable:
        icono = "🟡"
        mensaje = "debajo del umbral aceptable"
    else:
        icono = "🟢"
        mensaje = "cumple el umbral aceptable"

    return {
        "campo": campo,
        "dimension": "Completitud",
        "umbral_minimo": umbral_minimo,
        "umbral_aceptable": umbral_aceptable,
        "calidad": porcentaje_completitud,
        "icono": icono,
        "mensaje": mensaje
    }



# ### Calcula el promedio de la calidad por dimensión
# Esta función toma todas las dimensiones de calidad evaluadas en el conjunto de datos y calcula el promedio obtenido en la evaluación por cada una de estas, con esto se pretende dar un valor cuantitativo de la calidad por cada una de las dimensiones evaluadas a nivel de conjunto de datos y no del campo propiamente dicho

# In[8]:


def promedio_calidad_dimensiones(resultado):
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


# ### Calcula el promedio de la calidad por campo
# Esta función toma todas los campos del dataset evaluado y calcula el promedio obtenido en la evaluación por cada uno de estos, con esto se pretende dar un valor cuantitativo de la calidad por cada uno de los campos evaluadas.

# In[9]:


def promedio_calidad_campos(resultado, decimales=2):
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


# In[ ]:




