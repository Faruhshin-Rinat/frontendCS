import pandas as pd
import random
from concurrent.futures import ProcessPoolExecutor as Pool

def generate_files(n_files=5, n_rows=5):
    categories = ['A', 'B', 'C', 'D']
    for i in range(n_files):
        data = {
            "Категория": [random.choice(categories) for _ in range(n_rows)],
            "Значение": [round(random.uniform(0, 100), 3) for _ in range(n_rows)]
        }
        df = pd.DataFrame(data)
        df.to_csv(f"file_{i+1}.csv", index=False)

def process_file(filename):
    df = pd.read_csv(filename)
    result = df.groupby("Категория")["Значение"].agg(
        Медиана="median", Отклонение="std"
    ).reset_index()
    return filename, result

def main():
    generate_files()
    files = [f"file_{i+1}.csv" for i in range(5)]

    with Pool() as pool:
        results = list(pool.map(process_file, files))

    print("=== Результаты по каждому файлу ===")
    for filename, res in results:
        print(f"\nФайл {filename}")
        print(res.fillna("-"))

    all_results = pd.concat([res for _, res in results])

    final_stats = all_results.groupby("Категория")["Медиана"].agg(
        **{"Медиана медиан": "median", "Отклонение медиан": "std"}
    ).reset_index()

    print("\n=== Медиана из медиан и стандартное отклонение медиан ===")
    print(final_stats.fillna("-"))

if __name__ == "__main__":
    main()