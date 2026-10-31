import argparse
from src.generator import generate_data
from src.engine import DataQualityEngine, report

def main():
    parser = argparse.ArgumentParser(description="Configurable data quality pipeline")
    parser.add_argument("--generate", action="store_true", help="Generate reproducible sample data")
    parser.add_argument("--orders", type=int, default=1000, help="Number of orders to generate")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.generate:
        generate_data(orders=args.orders, seed=args.seed)
    result = DataQualityEngine().run()
    print("\nRUN SUMMARY")
    for key, value in result.items():
        print(f"{key}: {value}")
    report()

if __name__ == "__main__":
    main()
