import shutil
from pathlib import Path

import kagglehub


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    path = kagglehub.dataset_download(
        "igormerlinicomposer/online-casino-games-dataset-1-2m-records"
    )
    src = Path(path) / "online_casino_games_dataset_v2.csv"
    if not src.is_file():
        raise FileNotFoundError(f"Ожидался файл: {src}")

    dst = data_dir / "online_casino_games_dataset_v2.csv"
    shutil.copy2(src, dst)
    print(f"Скопировано: {dst}")


if __name__ == "__main__":
    main()
