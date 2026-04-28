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
    cur.execute(f"SELECT MAX({sk_name}) FROM {table_name}")
    result = cur.fetchone()[0]
    return result + 1 if result is not None else 1


def add_item(new_item: Item = None):
    new_item_sk = get_sk('item', 'i_item_sk')
    rec_start_date = f"{new_item.start_year}-01-01"
    cur.execute(
        "INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand,"
        " i_class, i_category, i_manufact, i_current_price, i_num_owned)"
        " VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)",
        (new_item_sk, new_item.item_id, rec_start_date, new_item.product_name,
         new_item.brand, new_item.category, new_item.manufact,
         new_item.current_price, new_item.num_owned)
    )


def add_customer(new_customer: Customer = None):
    address_parts = new_customer.address.split(",")
    street_num, street_name = address_parts[0].strip().split(" ", 1)
    city = address_parts[1].strip()
    state, zip_code = address_parts[2].strip().split(" ", 1)
    first_name, last_name = new_customer.name.split(" ", 1)

    new_customer_sk = get_sk('customer', 'c_customer_sk')
    new_address_sk = get_sk('customer_address', 'ca_address_sk')

    cur.execute(
        "INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name,"
        " ca_city, ca_state, ca_zip) VALUES (?, ?, ?, ?, ?, ?)",
        (new_address_sk, street_num, street_name, city, state, zip_code)
    )
    cur.execute(
        "INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name,"
        " c_email_address, c_current_addr_sk) VALUES (?, ?, ?, ?, ?, ?)",
        (new_customer_sk, new_customer.customer_id, first_name, last_name,
         new_customer.email, new_address_sk)
    )


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    if new_customer.customer_id:
        cur.execute(
            "UPDATE customer SET c_customer_id = ? WHERE TRIM(c_customer_id) = ?",
            (new_customer.customer_id, original_customer_id)
        )
        # subsequent address update should use new id if changed
        original_customer_id = new_customer.customer_id

    if new_customer.name:
        first_name, last_name = new_customer.name.split(" ", 1)
        cur.execute(
            "UPDATE customer SET c_first_name = ?, c_last_name = ? WHERE TRIM(c_customer_id) = ?",
            (first_name, last_name, original_customer_id)
        )

    if new_customer.email:
        cur.execute(
            "UPDATE customer SET c_email_address = ? WHERE TRIM(c_customer_id) = ?",
            (new_customer.email, original_customer_id)
        )

    if new_customer.address:
        address_parts = new_customer.address.split(",")
        street_num, street_name = address_parts[0].strip().split(" ", 1)
        city = address_parts[1].strip()
        state, zip_code = address_parts[2].strip().split(" ", 1)

        cur.execute(
            "SELECT ca_address_sk FROM customer_address"
            " WHERE TRIM(ca_street_number) = ? AND TRIM(ca_street_name) = ?"
            " AND TRIM(ca_city) = ? AND TRIM(ca_state) = ? AND TRIM(ca_zip) = ?",
            (street_num, street_name, city, state, zip_code)
        )
        result = cur.fetchone()
        if result:
            cur.execute(
                "UPDATE customer SET c_current_addr_sk = ? WHERE TRIM(c_customer_id) = ?",
                (result[0], original_customer_id)
            )
        else:
            new_address_sk = get_sk('customer_address', 'ca_address_sk')
            cur.execute(
                "INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name,"
                " ca_city, ca_state, ca_zip) VALUES (?, ?, ?, ?, ?, ?)",
                (new_address_sk, street_num, street_name, city, state, zip_code)
            )
            cur.execute(
                "UPDATE customer SET c_current_addr_sk = ? WHERE TRIM(c_customer_id) = ?",
                (new_address_sk, original_customer_id)
            )


def rent_item(item_id: str = None, customer_id: str = None):
    cur.execute(
        "INSERT INTO rental (item_id, customer_id, rental_date, due_date)"
        " VALUES (?, ?, CURDATE(), DATE_ADD(CURDATE(), INTERVAL 14 DAY))",
        (item_id, customer_id)
    )


def return_item(item_id: str = None, customer_id: str = None):
    cur.execute(
        "SELECT item_id, customer_id, rental_date, due_date FROM rental"
        " WHERE item_id = ? AND customer_id = ?",
        (item_id, customer_id)
    )
    row = cur.fetchone()
    cur.execute(
        "INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date)"
        " VALUES (?, ?, ?, ?, CURDATE())",
        (row[0], row[1], row[2], row[3])
    )
    cur.execute(
        "DELETE FROM rental WHERE item_id = ? AND customer_id = ?",
        (item_id, customer_id)
    )


def grant_extension(item_id: str = None, customer_id: str = None):
    cur.execute(
        "UPDATE rental SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY)"
        " WHERE item_id = ? AND customer_id = ?",
        (item_id, customer_id)
    )


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    cur.execute(
        "SELECT MAX(place_in_line) FROM waitlist WHERE item_id = ?",
        (item_id,)
    )
    result = cur.fetchone()[0]
    new_place = result + 1 if result is not None else 1
    cur.execute(
        "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)",
        (item_id, customer_id, new_place)
    )
    return new_place


def update_waitlist(item_id: str = None):
    cur.execute(
        "DELETE FROM waitlist WHERE place_in_line = 1 AND item_id = ?",
        (item_id,)
    )
    cur.execute(
        "UPDATE waitlist SET place_in_line = place_in_line - 1 WHERE item_id = ?",
        (item_id,)
    )


def number_in_stock(item_id: str = None) -> int:
    cur.execute("SELECT i_num_owned FROM item WHERE TRIM(i_item_id) = ?", (item_id,))
    result = cur.fetchone()
    if not result:
        return -1
    cur.execute("SELECT COUNT(*) FROM rental WHERE item_id = ?", (item_id,))
    return result[0] - cur.fetchone()[0]


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    cur.execute(
        "SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?",
        (item_id, customer_id)
    )
    result = cur.fetchone()
    return result[0] if result else -1


def line_length(item_id: str = None) -> int:
    cur.execute("SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item_id,))
    result = cur.fetchone()
    return result[0] if result else 0


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    conditions = []
    params = []
    op = "LIKE" if use_patterns else "="

    if filter_attributes.item_id is not None:
        conditions.append(f"TRIM(i_item_id) {op} ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.product_name is not None:
        conditions.append(f"TRIM(i_product_name) {op} ?")
        params.append(filter_attributes.product_name)
    if filter_attributes.brand is not None:
        conditions.append(f"TRIM(i_brand) {op} ?")
        params.append(filter_attributes.brand)
    if filter_attributes.category is not None:
        conditions.append(f"TRIM(i_category) {op} ?")
        params.append(filter_attributes.category)
    if filter_attributes.manufact is not None:
        conditions.append(f"TRIM(i_manufact) {op} ?")
        params.append(filter_attributes.manufact)
    if filter_attributes.current_price != -1:
        conditions.append("i_current_price = ?")
        params.append(filter_attributes.current_price)
    if filter_attributes.num_owned != -1:
        conditions.append("i_num_owned = ?")
        params.append(filter_attributes.num_owned)
    if min_price != -1:
        conditions.append("i_current_price >= ?")
        params.append(min_price)
    if max_price != -1:
        conditions.append("i_current_price <= ?")
        params.append(max_price)
    if min_start_year != -1:
        conditions.append("YEAR(i_rec_start_date) >= ?")
        params.append(min_start_year)
    if max_start_year != -1:
        conditions.append("YEAR(i_rec_start_date) <= ?")
        params.append(max_start_year)

    sql = ("SELECT TRIM(i_item_id), TRIM(i_product_name), TRIM(i_brand), TRIM(i_category),"
           " TRIM(i_manufact), i_current_price, YEAR(i_rec_start_date), i_num_owned FROM item")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    cur.execute(sql, params)
    return [Item(item_id=r[0], product_name=r[1], brand=r[2], category=r[3],
                 manufact=r[4], current_price=float(r[5]), start_year=r[6], num_owned=r[7])
            for r in cur.fetchall()]


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    conditions = []
    params = []
    op = "LIKE" if use_patterns else "="

    if filter_attributes.customer_id is not None:
        conditions.append(f"TRIM(c.c_customer_id) {op} ?")
        params.append(filter_attributes.customer_id)
    if filter_attributes.name is not None:
        conditions.append(f"TRIM(CONCAT(c.c_first_name, ' ', c.c_last_name)) {op} ?")
        params.append(filter_attributes.name)
    if filter_attributes.email is not None:
        conditions.append(f"TRIM(c.c_email_address) {op} ?")
        params.append(filter_attributes.email)
    if filter_attributes.address is not None:
        conditions.append(
            f"TRIM(CONCAT(ca.ca_street_number, ' ', ca.ca_street_name, ', ',"
            f" ca.ca_city, ', ', ca.ca_state, ' ', ca.ca_zip)) {op} ?"
        )
        params.append(filter_attributes.address)

    sql = ("SELECT TRIM(c.c_customer_id), TRIM(c.c_first_name), TRIM(c.c_last_name),"
           " TRIM(ca.ca_street_number), TRIM(ca.ca_street_name), TRIM(ca.ca_city),"
           " TRIM(ca.ca_state), TRIM(ca.ca_zip), TRIM(c.c_email_address)"
           " FROM customer c JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    cur.execute(sql, params)
    result = []
    for r in cur.fetchall():
        address = f"{r[3]} {r[4]}, {r[5]}, {r[6]} {r[7]}"
        result.append(Customer(customer_id=r[0], name=f"{r[1]} {r[2]}", address=address, email=r[8]))
    return result


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    conditions = []
    params = []

    if filter_attributes.item_id is not None:
        conditions.append("item_id = ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        conditions.append("customer_id = ?")
        params.append(filter_attributes.customer_id)
    if filter_attributes.rental_date is not None:
        conditions.append("rental_date = ?")
        params.append(filter_attributes.rental_date)
    if filter_attributes.due_date is not None:
        conditions.append("due_date = ?")
        params.append(filter_attributes.due_date)
    if min_rental_date is not None:
        conditions.append("rental_date >= ?")
        params.append(min_rental_date)
    if max_rental_date is not None:
        conditions.append("rental_date <= ?")
        params.append(max_rental_date)
    if min_due_date is not None:
        conditions.append("due_date >= ?")
        params.append(min_due_date)
    if max_due_date is not None:
        conditions.append("due_date <= ?")
        params.append(max_due_date)

    sql = "SELECT item_id, customer_id, rental_date, due_date FROM rental"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    cur.execute(sql, params)
    return [Rental(item_id=r[0].strip(), customer_id=r[1].strip(),
                   rental_date=str(r[2]), due_date=str(r[3]))
            for r in cur.fetchall()]


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    conditions = []
    params = []

    if filter_attributes.item_id is not None:
        conditions.append("item_id = ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        conditions.append("customer_id = ?")
        params.append(filter_attributes.customer_id)
    if filter_attributes.rental_date is not None:
        conditions.append("rental_date = ?")
        params.append(filter_attributes.rental_date)
    if filter_attributes.due_date is not None:
        conditions.append("due_date = ?")
        params.append(filter_attributes.due_date)
    if filter_attributes.return_date is not None:
        conditions.append("return_date = ?")
        params.append(filter_attributes.return_date)
    if min_rental_date is not None:
        conditions.append("rental_date >= ?")
        params.append(min_rental_date)
    if max_rental_date is not None:
        conditions.append("rental_date <= ?")
        params.append(max_rental_date)
    if min_due_date is not None:
        conditions.append("due_date >= ?")
        params.append(min_due_date)
    if max_due_date is not None:
        conditions.append("due_date <= ?")
        params.append(max_due_date)
    if min_return_date is not None:
        conditions.append("return_date >= ?")
        params.append(min_return_date)
    if max_return_date is not None:
        conditions.append("return_date <= ?")
        params.append(max_return_date)

    sql = "SELECT item_id, customer_id, rental_date, due_date, return_date FROM rental_history"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    cur.execute(sql, params)
    return [RentalHistory(item_id=r[0].strip(), customer_id=r[1].strip(),
                          rental_date=str(r[2]), due_date=str(r[3]), return_date=str(r[4]))
            for r in cur.fetchall()]


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    conditions = []
    params = []

    if filter_attributes.item_id is not None:
        conditions.append("item_id = ?")
        params.append(filter_attributes.item_id)
    if filter_attributes.customer_id is not None:
        conditions.append("customer_id = ?")
        params.append(filter_attributes.customer_id)
    if filter_attributes.place_in_line != -1:
        conditions.append("place_in_line = ?")
        params.append(filter_attributes.place_in_line)
    if min_place_in_line != -1:
        conditions.append("place_in_line >= ?")
        params.append(min_place_in_line)
    if max_place_in_line != -1:
        conditions.append("place_in_line <= ?")
        params.append(max_place_in_line)

    sql = "SELECT item_id, customer_id, place_in_line FROM waitlist"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    cur.execute(sql, params)
    return [Waitlist(item_id=r[0].strip(), customer_id=r[1].strip(), place_in_line=r[2])
            for r in cur.fetchall()]


def save_changes():
    conn.commit()


def close_connection():
    cur.close()
    conn.close()
