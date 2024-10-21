import json
import csv

def generate_csv(num_elements):
    filename = f"sample-{num_elements}.csv"
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['ts', 'key', 'value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(1, num_elements + 1):
            ts = i * 1000
            writer.writerow({'ts': ts, 'key': 0, 'value': 2000.0})

def main():
    # Load configuration from JSON
    with open('config.json') as json_file:
        config = json.load(json_file)
        num_elements = config['num_elements']

    # Generate CSV file
    generate_csv(num_elements)
    print(f"CSV file generated with {num_elements} elements.")

if __name__ == "__main__":
    main()
