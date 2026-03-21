from datetime import datetime
from utils.ist_time import get_ist_now
vehicle_db = {
    "TN41AY4048": {
        "owner_name": "Akalya Lakshmi S",
        "manufacturer": "Honda",
        "model": "Activa 6g",
        "fuel_type": "Petrol",
        "registration_date": "2021-10-18",
        "price": 85000
    },
    "TN42AF7215": {
        "owner_name": "Arjun Raj V",
        "manufacturer": "Volkswagen",
        "model": "Polo GT",
        "fuel_type": "Petrol",
        "registration_date": "2019-03-12",
        "price": 1200000
    },
    "TN37DZ777": {
        "owner_name": "Pravin Kumar J",
        "manufacturer": "Toyota",
        "model": "Innova Crysta",
        "fuel_type": "Diesel",
        "registration_date": "2021-01-20",
        "price": 3000000
    },
    "TN47CW2223": {
        "owner_name": "Karthikeyan D",
        "manufacturer": "Hyundai",
        "model": "Venue",
        "fuel_type": "Petrol",
        "registration_date": "2018-11-10",
        "price": 1150000
    },
    "TN90C5757": {
        "owner_name": "Vivekanadan S",
        "manufacturer": "Maruti Suzuki",
        "model": "Vitara Breeza",
        "fuel_type": "Diesel",
        "registration_date": "2020-05-05",
        "price": 1350000
    },
}

def calculate_resale_value(price, age):

    if age <= 2:
        percent = 0.80
    elif age <= 5:
        percent = 0.70
    elif age <= 8:
        percent = 0.60
    elif age <= 12:
        percent = 0.50
    else:
        return None

    return price * percent

def fetch_vehicle_details(reg_number: str):

    vehicle = vehicle_db.get(reg_number.upper())

    if not vehicle:
        return None

    year = int(vehicle["registration_date"].split("-")[0])
    vehicle_age = get_ist_now().year - year

    # MAX TENURE RULE
    if vehicle_age <= 3:
        max_tenure = 24
    elif vehicle_age <= 6:
        max_tenure = 18
    else:
        max_tenure = 12

    # VEHICLE PRICE
    price = vehicle["price"]

    # DEPRECIATION BASED ON AGE
    if vehicle_age <= 2:
        resale_percent = 0.80
    elif vehicle_age <= 5:
        resale_percent = 0.70
    elif vehicle_age <= 8:
        resale_percent = 0.60
    else:
        resale_percent = 0.50

    resale_value = price * resale_percent

    # LOAN TO VALUE 70%
    max_loan_amount = resale_value * 0.70

    vehicle_data = vehicle.copy()

    vehicle_data["vehicle_age"] = vehicle_age
    vehicle_data["max_tenure_allowed"] = max_tenure
    vehicle_data["market_price"] = price
    vehicle_data["resale_value"] = round(resale_value, 2)
    vehicle_data["max_loan_amount"] = round(max_loan_amount, 2)

    return vehicle_data