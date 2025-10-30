import pandas as pd

def filtrar_por_fechas(df, columna_fecha, fecha_inicio, fecha_fin):
    """
    Filtra un DataFrame entre dos fechas dadas.
    """
    df_filtrado = df[
        (df[columna_fecha] >= pd.to_datetime(fecha_inicio)) &
        (df[columna_fecha] <= pd.to_datetime(fecha_fin))
    ].copy()
    return df_filtrado

def generar_pivote(df, filas, columnas, valores, funcion_agrupadora):
    """
    Genera una tabla pivote según parámetros de entrada.
    """
    pivote = pd.pivot_table(
        df,
        index=filas,
        columns=columnas,
        values=valores,
        aggfunc=funcion_agrupadora,
        fill_value=0
    )
    return pivote

def guardar_en_postgresql(df, nombre_tabla, engine, if_exists='replace'):
    """
    Guarda un DataFrame en una tabla de PostgreSQL.
    """
    df.to_sql(nombre_tabla, engine, if_exists=if_exists, index=False)
    print(f"Tabla '{nombre_tabla}' guardada correctamente en PostgreSQL.")

def agregar_metricas_basicas(df):
    """
    Calcula columnas de venta, costo y ganancia en un DataFrame de órdenes.
    Requiere las columnas: quantityOrdered, priceEach, buyPrice.
    """
    df["venta"] = df["quantityOrdered"] * df["priceEach"]
    df["costo"] = df["quantityOrdered"] * df["buyPrice"]
    df["ganancia"] = df["venta"] - df["costo"]
    return df

def generar_top(
    df, columna_fecha, fecha_inicio, fecha_fin,
    columna_grupo, nombre_tabla, engine,
    n=10, if_exists='replace'
):
    """
    Genera un top N agrupado por 'columna_grupo' y lo guarda en PostgreSQL.
    Parámetros:
        df: DataFrame base con columnas de ventas
        columna_fecha: columna datetime para filtrar
        fecha_inicio, fecha_fin: rango de fechas
        columna_grupo: columna por la cual agrupar ('customerName' o 'productName')
        nombre_tabla: nombre de tabla destino en PostgreSQL
        engine: conexión SQLAlchemy
        n: número de filas del top
        if_exists: comportamiento si la tabla ya existe
    """

    # Paso 1: Filtrar por fechas
    df_filtrado = filtrar_por_fechas(df, columna_fecha, fecha_inicio, fecha_fin)

    # Paso 2: Calcular métricas (si no existen)
    columnas_necesarias = {"venta", "costo", "ganancia"}
    if not columnas_necesarias.issubset(df_filtrado.columns):
        df_filtrado = agregar_metricas_basicas(df_filtrado)

    # Paso 3: Agrupar por la columna solicitada
    resumen = (df_filtrado
               .groupby(columna_grupo)[["venta", "costo", "ganancia"]]
               .sum()
               .sort_values(by="venta", ascending=False)
               .head(n)
               .reset_index())

    # Paso 4: Guardar en PostgreSQL
    guardar_en_postgresql(resumen, nombre_tabla, engine, if_exists)

    print(f"Reporte '{nombre_tabla}' generado correctamente.")
    return resumen
