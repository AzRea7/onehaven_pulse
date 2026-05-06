from pipelines.quality.source_freshness import seed_freshness_rules


def main() -> None:
    count = seed_freshness_rules()
    print(f"Seeded or updated {count} source freshness rules.")


if __name__ == "__main__":
    main()
