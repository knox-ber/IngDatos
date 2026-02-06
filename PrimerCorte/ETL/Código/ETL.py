import pandas as pd


def ETL(input_path, output_path):
    # Cargar base
    df = pd.read_csv(input_path)

    # Eliminar columnas que no uso
    columnas = ["Code", "Estimated maternal deaths (Annotations)"]
    df = df.drop(columns=columnas)

    # Filtrar por los últimos 5 años
    """
        Aca una pequeña mejora respecto al excel que se me ocurrio para manejar menos datos pero de forma mas eficiente
    """
    ultimo_año = df["Year"].max()
    primer_año = ultimo_año - 4  # Este año cuenta
    df_filtrada = df[df["Year"] >= primer_año].copy()

    # Guardar la base transformada
    df_filtrada.to_csv(output_path, index=False)


if __name__ == "__main__":
    csv_input_path = "Base inicial - number-of-maternal-deaths-by-region.csv"
    csv_output_path = "Base ETL - number-of-maternal-deaths-by-region.csv"

    ETL(csv_input_path, csv_output_path)
