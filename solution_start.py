import argparse
import csv
import pandas as pd
import json
import glob
import os


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False,
                        default="C:\\Users\\priya\\OneDrive\\Documents\\revolve assignment\\Revolve Solutions - Python Assignment\\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\\input_data\\starter\\customers.csv")
    parser.add_argument('--products_location', required=False,
                        default="C:\\Users\\priya\\OneDrive\\Documents\\revolve assignment\\Revolve Solutions - Python Assignment\\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\\input_data\\starter\\products.csv")
    parser.add_argument('--transactions_location', required=False,
                        default="C:\\Users\\priya\\OneDrive\\Documents\\revolve assignment\\Revolve Solutions - Python Assignment\\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\\input_data\\starter\\transactions\\")
    parser.add_argument('--output_location', required=False,
                        default="C:\\Users\\priya\\OneDrive\\Documents\\revolve assignment\\Revolve Solutions - Python Assignment\\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\\output")
    return vars(parser.parse_args())


def read_csv(file_path: str) -> pd.DataFrame:
    # Implement function to read CSV file and return data as a list
    csv_data = pd.read_csv(file_path)
    return csv_data


def read_json_lines(directory_path: str) -> pd.DataFrame:
    # Implement function to read JSON lines from a directory and return data as a list
    json_data = []
    # Reading the json files
    file_pattern = os.path.join(directory_path, "*", "transactions.json")
    json_files = glob.glob(file_pattern)

    print(f"Found {len(json_files)} JSON files in {file_pattern}")

    for json_file in json_files:
        try:
            with open(json_file, "r") as file:
                lines = file.readlines()
                for line in lines:
                    try:
                        json_object = json.loads(line)
                        json_data.append(json_object)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in file {json_file}: {e}")
        except FileNotFoundError:
            print(f"File not found: {json_file}")
    # Convert the list of JSON strings to a DataFrame
    data_frame = pd.json_normalize(json_data)
    # Unnest Basket because it is a list of dictionary
    basket_df = pd.json_normalize(data_frame['basket'].explode())
    # Concatenating data_frame and basket_df
    result_df = pd.concat([data_frame, basket_df], axis=1).drop('basket', axis=1)
    return result_df


def collating_data(customers_data, products_data, transactions_data: pd.DataFrame) -> pd.DataFrame:
    merged_data = pd.merge(transactions_data, customers_data, on="customer_id", how="left")
    merged_data = pd.merge(merged_data, products_data, on='product_id', how='left')
    result_df = merged_data.groupby(
        ["customer_id", "loyalty_score", "product_id", "product_category"]).size().reset_index(
        name="purchase_count").sort_values(by="purchase_count")
    return result_df


def generate_output_json(result_df: pd.DataFrame, output_location: str) -> None:
    # Implement function to generate the output JSON file
    try:
        # Convert DataFrame to JSON string with records orientation and line-delimited format
        json_data = result_df.to_json(orient='records', lines=True)

        # Write JSON data to the specified output path
        with open(output_location, 'w+') as json_file:
            json_file.write(json_data)

        print(f"Output JSON file successfully created at: {output_location}")
    except Exception as e:
        print(f"Error generating output JSON: {e}")


def main():
    params = get_params()
    customers_data = read_csv(params['customers_location'])
    products_data = read_csv(params['products_location'])
    transactions_data = read_json_lines(params['transactions_location'])

    # print(transactions_data)
    # print(products_data[:5])
    # print(customers_data[:5])
    result = collating_data(customers_data, products_data, transactions_data)

    # Define the output location
    # output_location = params.get('output_location', "C:\\Users\\priya\\OneDrive\\Documents\\revolve assignment\\Revolve Solutions - Python Assignment\\python-assignment-level2-6ed53b4e828af18bc24b1770a3a3e3e70706e785\\output")
    output_location = os.path.join(params['output_location'], 'output.json')
    # Call the function to generate the output JSON
    generate_output_json(result, output_location)



if __name__ == "__main__":
    main()
