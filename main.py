from src.downloaders.kamis import main as kamis_main
from src.downloaders.knbs import main as knbs_main
from src.downloaders.synthetic_data import main as synthetic_main
def main():
    kamis_main()
    print("Kamis Extraction Done!")
    knbs_main()
    print("Knbs Extraction Done!")
    synthetic_main()
    print("Synthetic data creation Done!")


if __name__ == "__main__":
    main()
