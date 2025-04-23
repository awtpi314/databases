import os
import re
import argparse
from collections import defaultdict


def parse_insert_statement(line):
    """Parse an INSERT statement to extract table name and values."""
    # Match the basic structure of an INSERT statement
    match = re.match(
        r"INSERT\s+INTO\s+([^\s(]+)\s*(\([^)]+\))?\s*VALUES\s*\((.*)\);?",
        line,
        re.IGNORECASE,
    )

    if not match:
        return None, None, None

    table_name = match.group(1)
    columns = match.group(2)  # This might be None if not specified
    values = match.group(3)

    return table_name, columns, values


def batch_insert_statements(input_file, output_file, batch_size=1000):
    """Convert individual INSERT statements to batched INSERT statements."""
    # Dictionary to store INSERT statements by table
    inserts_by_table = defaultdict(list)

    # Read the input file and parse each INSERT statement
    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or not line.upper().startswith("INSERT"):
                continue

            table_name, columns, values = parse_insert_statement(line)
            if table_name is None:
                continue

            inserts_by_table[(table_name, columns)].append(values)

    # Write batched INSERT statements to the output file
    with open(output_file, "w") as f:
        for (table_name, columns), values_list in inserts_by_table.items():
            for i in range(0, len(values_list), batch_size):
                batch = values_list[i : i + batch_size]
                col_str = f" {columns} " if columns else " "
                values_str = "),\n(".join(batch)
                f.write(f"INSERT INTO {table_name}{col_str}VALUES\n({values_str});\n\n")


def main():
    parser = argparse.ArgumentParser(
        description="Convert individual INSERT statements to batched INSERT statements"
    )
    parser.add_argument(
        "input_file", help="Input SQL file with individual INSERT statements"
    )
    parser.add_argument(
        "output_file", help="Output SQL file for batched INSERT statements"
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows per batch (default: 1000)",
    )

    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist")
        return

    batch_insert_statements(args.input_file, args.output_file, args.batch_size)
    print(
        f"Successfully converted INSERT statements to batched format in '{args.output_file}'"
    )


if __name__ == "__main__":
    main()
