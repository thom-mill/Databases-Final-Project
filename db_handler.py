from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()

def get_sk(table_name: str, sk_name: str) -> int:
    cur.execute(f"SELECT MAX({sk_name}) FROM {table_name};")
    result = cur.fetchone()[0]
    return result+1 if result is not None else 1

def add_item(new_item: Item = None):
    new_item_sk = get_sk('item', 'i_item_sk')

    insert_query = f"""
                    INSERT INTO item
                    VALUES ({new_item_sk}, '{new_item.item_id}', {new_item.start_year}, 
                            '{new_item.product_name}', '{new_item.brand}', '{new_item.category}',
                            '{new_item.manufact}', {new_item.current_price}, {new_item.num_owned});
                    """
    cur.execute(insert_query)


def add_customer(new_customer: Customer = None):
    #parse the address into 5 components
    customer_address = new_customer.address.split(",")
    street_num, street_name = customer_address[0].split(" ", 1)
    city = customer_address[1]
    state, zip_code =customer_address[2].split(" ", 1);
    #parse the name into first and last
    first_name, last_name = new_customer.name.split(" ")

    new_customer_sk = get_sk('customer', 'c_customer_sk')
    new_customer_address_sk = get_sk('customer_address', 'ca_address_sk')
    
    insert_address_query = f"""INSERT INTO customer_address VALUES (
        {new_customer_address_sk}, '{street_num}', '{street_name}', 
        '{city}', '{state}', '{zip_code}')"""

    insert_customer_query = f"""INSERT INTO customer VALUES (
        {new_customer_sk}, '{new_customer.customer_id}', '{first_name}', '{last_name}', 
        '{new_customer.email}', {new_customer_address_sk})"""

    cur.execute(insert_address_query)
    cur.execute(insert_customer_query)


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    updates = {}
    new_address = ""
    if new_customer.customer_id:
        updates["c_customer_id"] = new_customer.customer_id
    if new_customer.name:
        first_name, last_name = new_customer.name.split(" ")
        updates["c_first_name"] = first_name
        updates["c_last_name"] = last_name
    if new_customer.email:
        updates["c_email_address"] = new_customer.email
    if new_customer.address:
        new_address = new_customer.address 
    #loop through changing all non None values
    for key, value in updates.items():
        edit_query = f"""
            UPDATE customer c
            SET {key} = '{value}'
            WHERE c.c_customer_id = '{original_customer_id}';
            """
        cur.execute(edit_query)
    #update address
    #if exists in db, change sk
    #if not exists, create a new sk and add it in
    if new_address:
        address_parts = new_address.split(",")
        street_num, street_name = address_parts[0].split(' ', 1)
        city = address_parts[1]
        state, zip_code = address_parts[2].split(' ', 1)

        cur.execute(f"""SELECT ca_address_sk FROM customer_address ca
                        WHERE ca.ca_street_number = '{street_num}' AND ca.ca_street_name = '{street_name}' AND
                              ca.ca_city = '{city}' AND ca.ca_state = '{state}' AND ca.ca_zip_code = '{zip_code}';"""
        )
        result = cur.fetchone()
        if result:
            cur.execute(f"""UPDATE customer c
                            SET c.c_current_addr_sk = {result[0]}
                            WHERE c.c_customer_id = '{original_customer_id}';"""
            )
        else:
            new_address_sk = get_sk('customer_address', 'ca_address_sk')
            cur.execute(f"""INSERT INTO customer_address VALUES (
                            {new_address_sk}, '{street_num}', '{street_name}', 
                            '{city}', '{state}', '{zip_code}');"""
            )
            cur.execute(f"""UPDATE customer c
                            SET c.c_current_addr_sk = {new_address_sk}
                            WHERE c.c_customer_id = '{original_customer_id}';"""
            )



def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """

    query = f"""
        INSERT INTO rental
        VALUES ('{item_id}', '{customer_id}', CURDATE(), DATE_ADD(CURDATE(), INTERVAL 14 DAY));
        """
    cur.execute(query)


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    cur.execute(f"""SELECT MAX(place_in_line) FROM waitlist WHERE item_id = '{item_id}';""")
    result = cur.fetchone()[0]
    new_place_in_line = result + 1 if result is not None else 1

    query = f"""
        INSERT INTO waitlist
        VALUES ('{item_id}', '{customer_id}', {new_place_in_line});
        """
    cur.execute(query)
    return new_place_in_line

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    cur.execute(f"""
        DELETE FROM waitlist
        WHERE place_in_line = 1 AND item_id = '{item_id}';"""
        )
    cur.execute(f"""
        UPDATE waitlist
        SET place_in_line = place_in_line - 1
        WHERE item_id = '{item_id}';
        """)


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    cur.execute("SELECT * FROM rental r WHERE r.item_id = '{item_id}' AND r.customer_id = '{customer_id}';")
    results = cur.fetchone()

    query = f"""
        INSERT INTO rental_history
        VALUES ('{item_id}', '{customer_id}', '{results[2]}', '{results[3]}', CURDATE());
        """
    cur.execute(query)
    cur.execute(f"""
        DELETE FROM rental
        WHERE item_id = '{item_id}' AND customer_id = '{customer_id}';
        """)


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    cur.execute(f"""
        UPDATE rental
        SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY)
        WHERE item_id = '{item_id}' AND customer_id = '{customer_id}';
        """)


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    return []


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    cur.execute("SELECT i.num_owned FROM item i WHERE i.i_item_id = '{item_id}';")
    result = cur.fetchone()
    if not result:
        return -1
    
    cur.execute("SELECT COUNT(*) FROM rental r WHERE r.item_id = '{item_id}';")
    active_rentals = cur.fetchone()[0]
    return result[0] - active_rentals


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    raise NotImplementedError("you must implement this function")


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    raise NotImplementedError("you must implement this function")


def save_changes():
    conn.commit()
    


def close_connection():
    conn.close()

