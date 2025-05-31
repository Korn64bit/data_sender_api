from flask import Flask, jsonify
import datetime
import random
import uuid # To generate a unique ID for each data point
import csv # For reading CSV files
import time

# Initialize the Flask application
app = Flask(__name__)

# --- CSV Data Handling ---
CSV_FILE_PATH = "flask/ai4i2020.csv"  # Path to your CSV file
sensor_data_from_csv = []          # Will store data loaded from CSV
current_csv_index = 0              # Tracks the current row to serve

def load_csv_data():
    """Loads data from the CSV file into memory."""
    global sensor_data_from_csv, current_csv_index
    try:
        with open(CSV_FILE_PATH, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file) # Reads rows as dictionaries
            
            # Basic check for header
            if not reader.fieldnames:
                print(f"Error: CSV file '{CSV_FILE_PATH}' is empty or has no header row.")
                sensor_data_from_csv = []
                return
            
            sensor_data_from_csv = list(reader) # Convert reader to a list of dictionaries
            if not sensor_data_from_csv:
                print(f"Warning: CSV file '{CSV_FILE_PATH}' contains no data rows after the header.")
            else:
                print(f"Successfully loaded {len(sensor_data_from_csv)} rows from {CSV_FILE_PATH}.")
        current_csv_index = 0 # Reset index after loading
    except FileNotFoundError:
        print(f"Error: CSV file not found at '{CSV_FILE_PATH}'. Please create the file or update the path.")
        print("The API will return errors until the CSV is available and loaded.")
        sensor_data_from_csv = []
    except Exception as e:
        print(f"An error occurred while loading or parsing CSV data: {e}")
        sensor_data_from_csv = []

def get_next_csv_data_point():
    """
    Retrieves the next data point from the loaded CSV data.
    The data point will be a dictionary containing all columns from the CSV row,
    plus a unique 'id' and a 'timestamp' (either from CSV or generated).
    """
    global current_csv_index
    if not sensor_data_from_csv:
        return None # No data loaded

    # Get the current row from the CSV data (it's already a dictionary)
    row_from_csv = sensor_data_from_csv[current_csv_index]

    # Create a new dictionary to avoid modifying the cached list of dicts directly
    # and to add our API-specific fields.
    data_point = dict(row_from_csv) 
    
    # Add a unique ID for this specific serving of the data point
    data_point["id"] = str(uuid.uuid4())
    
    # Ensure a timestamp exists. If not in CSV, add current UTC time.
    if "timestamp" not in data_point:
        data_point["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
    
    # Move to the next row, looping back to the start if at the end
    current_csv_index = (current_csv_index + 1) % len(sensor_data_from_csv)
    # time.sleep(5)
    
    return data_point

# --- API Endpoints ---
@app.route('/data', methods=['GET'])
def get_data():
    """API endpoint to get the latest single data point from CSV."""
    if not sensor_data_from_csv:
        return jsonify({"error": "No data loaded from CSV. Please check server logs and CSV file."}), 500
    
    current_data = get_next_csv_data_point()
    if current_data is None: 
        return jsonify({"error": "Failed to retrieve data point, data source might be empty or an issue occurred."}), 500
        
    print(f"Sending data: {current_data}") # For logging on the server side
    return jsonify(current_data) # Returns data as JSON

@app.route('/bulk-data', methods=['GET'])
def get_bulk_data():
    """API endpoint to get a small batch of data points from CSV."""
    if not sensor_data_from_csv:
        return jsonify({"error": "No data loaded from CSV. Please check server logs and CSV file."}), 500

    num_points_to_send = random.randint(3, 7)
    data_points_batch = []
    
    # Ensure we don't try to get more points than available if CSV is very small,
    # though get_next_csv_data_point handles looping.
    actual_points_to_send = min(num_points_to_send, len(sensor_data_from_csv))
    
    for _ in range(actual_points_to_send):
        point = get_next_csv_data_point()
        if point: 
            data_points_batch.append(point)
        else: # Safety break, e.g. if sensor_data_from_csv became empty concurrently
            break 
            
    if not data_points_batch and sensor_data_from_csv: # If we intended to send points but couldn't
        return jsonify({"error": "Failed to retrieve bulk data points, data source might be empty or an issue occurred."}), 500

    print(f"Sending bulk data: {len(data_points_batch)} points")
    return jsonify(data_points_batch)

if __name__ == '__main__':
    # Load data from CSV when the application starts
    load_csv_data()
    
    # Runs the Flask development server.
    # host='0.0.0.0' makes it accessible from other devices on your network
    # Use a specific port, e.g., 5001, if 5000 is in use.
    app.run(host='0.0.0.0', port=5001, debug=True)
