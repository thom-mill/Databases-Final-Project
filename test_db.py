"""
Automated test suite for db_handler.py
Run: python3 test_db.py
"""
import db_handler as db
from models.Item import Item
from models.Customer import Customer
from models.Rental import Rental
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from datetime import date, timedelta

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

results = []

def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    msg = f"[{status}] {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    results.append((name, condition))

def section(title):
    print(f"\n{'='*50}\n{title}\n{'='*50}")

# ── Test IDs (unique enough to not clash with real data) ──
TEST_ITEM_ID     = "TESTITEM0000001A"
TEST_ITEM_ID2    = "TESTITEM0000002A"
TEST_CUST_ID     = "TESTCUST0000001A"
TEST_CUST_ID2    = "TESTCUST0000002A"

# ─────────────────────────────────────────────
section("1. get_filtered_items — basic search")
# ─────────────────────────────────────────────
items = db.get_filtered_items(Item())
check("get_filtered_items no filter returns results", len(items) > 0)
check("Item objects have item_id", all(i.item_id for i in items[:5]))
check("Item objects have product_name", all(i.product_name for i in items[:5]))
check("Item current_price is float", all(isinstance(i.current_price, float) for i in items[:5]))
check("Item start_year is int", all(isinstance(i.start_year, int) for i in items[:5]))
check("Item num_owned is int", all(isinstance(i.num_owned, int) for i in items[:5]))
check("No surrogate keys exposed", all(not hasattr(i, 'i_item_sk') for i in items[:5]))

# pick a real item for later use
real_item = items[0]

section("1b. get_filtered_items — filter by item_id exact")
res = db.get_filtered_items(Item(item_id=real_item.item_id))
check("Exact item_id filter returns 1 result", len(res) == 1)
check("Returned item matches", res[0].item_id == real_item.item_id)

section("1c. get_filtered_items — price range")
min_p, max_p = 5.0, 50.0
res = db.get_filtered_items(Item(), min_price=min_p, max_price=max_p)
check("Price range filter returns results", len(res) > 0)
check("All prices within range", all(min_p <= i.current_price <= max_p for i in res))

section("1d. get_filtered_items — year range")
res = db.get_filtered_items(Item(), min_start_year=2000, max_start_year=2002)
check("Year range filter returns results", len(res) > 0)
check("All years within range", all(2000 <= i.start_year <= 2002 for i in res))

section("1e. get_filtered_items — LIKE pattern")
res = db.get_filtered_items(Item(brand="%"), use_patterns=True)
check("LIKE % returns results", len(res) > 0)

# ─────────────────────────────────────────────
section("2. get_filtered_customers — basic search")
# ─────────────────────────────────────────────
custs = db.get_filtered_customers(Customer())
check("get_filtered_customers no filter returns results", len(custs) > 0)
check("Customer has customer_id", all(c.customer_id for c in custs[:5]))
check("Customer has name", all(c.name for c in custs[:5]))
check("Customer has address", all(c.address for c in custs[:5]))
check("Customer has email", all(c.email for c in custs[:5]))
check("Address format has comma", all("," in c.address for c in custs[:5]))

real_cust = custs[0]

section("2b. get_filtered_customers — exact customer_id")
res = db.get_filtered_customers(Customer(customer_id=real_cust.customer_id))
check("Exact customer_id returns 1", len(res) == 1)
check("customer_id matches", res[0].customer_id == real_cust.customer_id)

section("2c. get_filtered_customers — LIKE name pattern")
res = db.get_filtered_customers(Customer(name="%on%"), use_patterns=True)
check("LIKE name pattern returns results", len(res) > 0)

section("2d. get_filtered_customers — filter by email")
res = db.get_filtered_customers(Customer(email=real_cust.email))
check("filter by email returns result", len(res) >= 1)

section("2e. get_filtered_customers — no match returns empty list")
res = db.get_filtered_customers(Customer(customer_id="ZZZNOMATCH00000Z"))
check("no match returns empty list", res == [])

section("2f. get_filtered_customers — address format correct")
c = db.get_filtered_customers(Customer(customer_id=real_cust.customer_id))[0]
parts = c.address.split(", ")
check("address has 3 comma-separated parts", len(parts) == 3)
check("address state+zip in last part", len(parts[2].strip()) > 0)

# ─────────────────────────────────────────────
section("3. add_item")
# ─────────────────────────────────────────────
new_item = Item(item_id=TEST_ITEM_ID, product_name="Test Product", brand="TestBrand",
                category="TestCat", manufact="TestMfg", current_price=9.99,
                start_year=2024, num_owned=3)
try:
    db.add_item(new_item)
    db.save_changes()
    res = db.get_filtered_items(Item(item_id=TEST_ITEM_ID))
    check("add_item inserts item", len(res) == 1)
    check("add_item product_name correct", res[0].product_name == "Test Product")
    check("add_item start_year correct", res[0].start_year == 2024)
    check("add_item num_owned correct", res[0].num_owned == 3)
    check("add_item current_price correct", abs(res[0].current_price - 9.99) < 0.01)
except Exception as e:
    check("add_item no exception", False, str(e))

section("3b. add_item — brand/category/manufact fields round-trip")
res = db.get_filtered_items(Item(item_id=TEST_ITEM_ID))
check("add_item brand correct", res[0].brand == "TestBrand")
check("add_item category correct", res[0].category == "TestCat")
check("add_item manufact correct", res[0].manufact == "TestMfg")

section("3c. add_item — duplicate rejected by app logic (item_exists check)")
res = db.get_filtered_items(Item(item_id=TEST_ITEM_ID))
check("Duplicate item_id already exists (app would reject)", len(res) == 1)

section("3d. get_filtered_items — filter by brand exact")
res = db.get_filtered_items(Item(brand="TestBrand"))
check("filter by brand returns test item", any(i.item_id == TEST_ITEM_ID for i in res))

section("3e. get_filtered_items — no match returns empty list")
res = db.get_filtered_items(Item(item_id="ZZZNOMATCH00000Z"))
check("no match returns empty list", res == [])

# ─────────────────────────────────────────────
section("4. add_customer")
# ─────────────────────────────────────────────
new_cust = Customer(customer_id=TEST_CUST_ID, name="Test User",
                    email="test@test.com", address="123 Main St, Gainesville, FL 32601")
try:
    db.add_customer(new_cust)
    db.save_changes()
    res = db.get_filtered_customers(Customer(customer_id=TEST_CUST_ID))
    check("add_customer inserts customer", len(res) == 1)
    check("add_customer name correct", res[0].name == "Test User")
    check("add_customer email correct", res[0].email == "test@test.com")
    check("add_customer address correct", "123" in res[0].address and "Gainesville" in res[0].address)
except Exception as e:
    check("add_customer no exception", False, str(e))

# ─────────────────────────────────────────────
section("5. number_in_stock")
# ─────────────────────────────────────────────
stock = db.number_in_stock(TEST_ITEM_ID)
check("number_in_stock returns 3 (none rented)", stock == 3)
check("number_in_stock nonexistent item returns -1", db.number_in_stock("DOESNOTEXIST0000") == -1)

# ─────────────────────────────────────────────
section("6. rent_item")
# ─────────────────────────────────────────────
try:
    db.rent_item(TEST_ITEM_ID, TEST_CUST_ID)
    db.save_changes()
    rentals = db.get_filtered_rentals(Rental(item_id=TEST_ITEM_ID, customer_id=TEST_CUST_ID))
    check("rent_item inserts rental", len(rentals) == 1)
    check("rental_date is today", rentals[0].rental_date == str(date.today()))
    expected_due = str(date.today() + timedelta(days=14))
    check("due_date is today+14", rentals[0].due_date == expected_due)
    check("number_in_stock decremented", db.number_in_stock(TEST_ITEM_ID) == 2)
except Exception as e:
    check("rent_item no exception", False, str(e))

# ─────────────────────────────────────────────
section("7. get_filtered_rentals")
# ─────────────────────────────────────────────
res = db.get_filtered_rentals(Rental(item_id=TEST_ITEM_ID))
check("get_filtered_rentals by item_id", len(res) >= 1)
today_str = str(date.today())
res = db.get_filtered_rentals(Rental(), min_rental_date=today_str, max_rental_date=today_str)
check("get_filtered_rentals date range", len(res) >= 1)
check("Rental item_id is str", all(isinstance(r.item_id, str) for r in res))
check("Rental rental_date is str YYYY-MM-DD", all(len(r.rental_date) == 10 for r in res))

# ─────────────────────────────────────────────
section("8. grant_extension")
# ─────────────────────────────────────────────
try:
    db.grant_extension(TEST_ITEM_ID, TEST_CUST_ID)
    db.save_changes()
    rentals = db.get_filtered_rentals(Rental(item_id=TEST_ITEM_ID, customer_id=TEST_CUST_ID))
    expected_due = str(date.today() + timedelta(days=28))
    check("grant_extension adds 14 days", rentals[0].due_date == expected_due)
except Exception as e:
    check("grant_extension no exception", False, str(e))

# ─────────────────────────────────────────────
section("9. waitlist & line functions")
# ─────────────────────────────────────────────
# Add second test customer for waitlist
new_cust2 = Customer(customer_id=TEST_CUST_ID2, name="Wait Listed",
                     email="wait@test.com", address="456 Oak Ave, Tampa, FL 33601")
try:
    db.add_customer(new_cust2)
    db.save_changes()
except Exception as e:
    check("add_customer2 no exception", False, str(e))

check("line_length 0 before waitlist", db.line_length(TEST_ITEM_ID) == 0)
check("place_in_line -1 before waitlist", db.place_in_line(TEST_ITEM_ID, TEST_CUST_ID2) == -1)

try:
    pos = db.waitlist_customer(TEST_ITEM_ID, TEST_CUST_ID2)
    db.save_changes()
    check("waitlist_customer returns 1", pos == 1)
    check("line_length is 1", db.line_length(TEST_ITEM_ID) == 1)
    check("place_in_line is 1", db.place_in_line(TEST_ITEM_ID, TEST_CUST_ID2) == 1)
except Exception as e:
    check("waitlist_customer no exception", False, str(e))

section("9b. get_filtered_waitlist")
res = db.get_filtered_waitlist(Waitlist(item_id=TEST_ITEM_ID))
check("get_filtered_waitlist by item_id", len(res) == 1)
check("Waitlist place_in_line is int", isinstance(res[0].place_in_line, int))
res = db.get_filtered_waitlist(Waitlist(), min_place_in_line=1, max_place_in_line=1)
check("get_filtered_waitlist place range", len(res) >= 1)

section("9c. update_waitlist — multiple positions shift correctly")
# Add two more customers to test shifting
new_cust3 = Customer(customer_id="TESTCUST0000003A", name="Third Person",
                     email="third@test.com", address="789 Pine Rd, Orlando, FL 32801")
new_cust4 = Customer(customer_id="TESTCUST0000004A", name="Fourth Person",
                     email="fourth@test.com", address="321 Elm St, Miami, FL 33101")
try:
    db.add_customer(new_cust3)
    db.add_customer(new_cust4)
    db.save_changes()
    db.waitlist_customer(TEST_ITEM_ID, "TESTCUST0000003A")
    db.waitlist_customer(TEST_ITEM_ID, "TESTCUST0000004A")
    db.save_changes()
    check("line_length is 3 before update", db.line_length(TEST_ITEM_ID) == 3)
    db.update_waitlist(TEST_ITEM_ID)
    db.save_changes()
    check("update_waitlist removes position 1", db.line_length(TEST_ITEM_ID) == 2)
    check("cust2 removed (was position 1)", db.place_in_line(TEST_ITEM_ID, TEST_CUST_ID2) == -1)
    check("cust3 shifted to position 1", db.place_in_line(TEST_ITEM_ID, "TESTCUST0000003A") == 1)
    check("cust4 shifted to position 2", db.place_in_line(TEST_ITEM_ID, "TESTCUST0000004A") == 2)
    # clear remaining waitlist
    db.update_waitlist(TEST_ITEM_ID)
    db.update_waitlist(TEST_ITEM_ID)
    db.save_changes()
    check("waitlist empty after clearing", db.line_length(TEST_ITEM_ID) == 0)
except Exception as e:
    check("update_waitlist shift no exception", False, str(e))

# ─────────────────────────────────────────────
section("10. return_item")
# ─────────────────────────────────────────────
try:
    db.return_item(TEST_ITEM_ID, TEST_CUST_ID)
    db.save_changes()
    rentals = db.get_filtered_rentals(Rental(item_id=TEST_ITEM_ID, customer_id=TEST_CUST_ID))
    check("return_item removes from rental", len(rentals) == 0)
    check("number_in_stock restored", db.number_in_stock(TEST_ITEM_ID) == 3)
    histories = db.get_filtered_rental_histories(RentalHistory(item_id=TEST_ITEM_ID, customer_id=TEST_CUST_ID))
    check("return_item adds to rental_history", len(histories) >= 1)
    check("return_date is today", histories[-1].return_date == str(date.today()))
except Exception as e:
    check("return_item no exception", False, str(e))

# ─────────────────────────────────────────────
section("11. get_filtered_rental_histories")
# ─────────────────────────────────────────────
res = db.get_filtered_rental_histories(RentalHistory())
check("get_filtered_rental_histories no filter returns results", len(res) > 0)
today_str = str(date.today())
res = db.get_filtered_rental_histories(RentalHistory(), min_return_date=today_str, max_return_date=today_str)
check("get_filtered_rental_histories return_date range", len(res) >= 1)
check("RentalHistory return_date is str", all(isinstance(r.return_date, str) for r in res[:5]))

# ─────────────────────────────────────────────
section("12. edit_customer — name and email")
try:
    db.edit_customer(TEST_CUST_ID, Customer(email="updated@test.com", name="Updated Name"))
    db.save_changes()
    res = db.get_filtered_customers(Customer(customer_id=TEST_CUST_ID))
    check("edit_customer updates email", res[0].email == "updated@test.com")
    check("edit_customer updates name", res[0].name == "Updated Name")
except Exception as e:
    check("edit_customer no exception", False, str(e))

section("12b. edit_customer — address update")
try:
    db.edit_customer(TEST_CUST_ID, Customer(address="999 New Ave, Jacksonville, FL 32099"))
    db.save_changes()
    res = db.get_filtered_customers(Customer(customer_id=TEST_CUST_ID))
    check("edit_customer updates address city", "Jacksonville" in res[0].address)
    check("edit_customer updates address street", "999" in res[0].address)
except Exception as e:
    check("edit_customer address no exception", False, str(e))

section("12c. edit_customer — only None fields unchanged")
try:
    before = db.get_filtered_customers(Customer(customer_id=TEST_CUST_ID))[0]
    db.edit_customer(TEST_CUST_ID, Customer(email="only_email@test.com"))
    db.save_changes()
    after = db.get_filtered_customers(Customer(customer_id=TEST_CUST_ID))[0]
    check("edit_customer unchanged name when not provided", after.name == before.name)
    check("edit_customer changed email", after.email == "only_email@test.com")
except Exception as e:
    check("edit_customer partial update no exception", False, str(e))

section("12d. edit_customer — change customer_id")
try:
    db.edit_customer(TEST_CUST_ID, Customer(customer_id="TESTCUST0000001B"))
    db.save_changes()
    res_new = db.get_filtered_customers(Customer(customer_id="TESTCUST0000001B"))
    res_old = db.get_filtered_customers(Customer(customer_id=TEST_CUST_ID))
    check("edit_customer new customer_id found", len(res_new) == 1)
    check("edit_customer old customer_id gone", len(res_old) == 0)
    # rename back for cleanup
    db.edit_customer("TESTCUST0000001B", Customer(customer_id=TEST_CUST_ID))
    db.save_changes()
except Exception as e:
    check("edit_customer change id no exception", False, str(e))

# ─────────────────────────────────────────────
section("13. get_filtered_rentals — no match returns empty list")
res = db.get_filtered_rentals(Rental(item_id="ZZZNOMATCH00000Z"))
check("get_filtered_rentals no match returns empty", res == [])

section("13b. get_filtered_rental_histories — due_date range filter")
res_all = db.get_filtered_rental_histories(RentalHistory())
if res_all:
    sample_due = res_all[0].due_date
    res = db.get_filtered_rental_histories(RentalHistory(), min_due_date=sample_due, max_due_date=sample_due)
    check("rental_history due_date range filter works", len(res) >= 1)
    check("all due_dates match range", all(r.due_date == sample_due for r in res))

section("13c. get_filtered_waitlist — no match returns empty list")
res = db.get_filtered_waitlist(Waitlist(item_id="ZZZNOMATCH00000Z"))
check("get_filtered_waitlist no match returns empty", res == [])

section("14. Cleanup test data")
# ─────────────────────────────────────────────
try:
    import mariadb
    from MARIADB_CREDS import DB_CONFIG
    conn2 = mariadb.connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"],
                            host=DB_CONFIG["host"], port=DB_CONFIG["port"],
                            database=DB_CONFIG["database"])
    cur2 = conn2.cursor()
    cur2.execute("DELETE FROM rental_history WHERE item_id = ?", (TEST_ITEM_ID,))
    cur2.execute("DELETE FROM rental WHERE item_id = ?", (TEST_ITEM_ID,))
    cur2.execute("DELETE FROM waitlist WHERE item_id = ?", (TEST_ITEM_ID,))
    cur2.execute("DELETE FROM item WHERE TRIM(i_item_id) = ?", (TEST_ITEM_ID,))
    for cid in (TEST_CUST_ID, TEST_CUST_ID2, "TESTCUST0000003A", "TESTCUST0000004A"):
        cur2.execute("DELETE FROM customer WHERE TRIM(c_customer_id) = ?", (cid,))
    conn2.commit()
    conn2.close()
    check("Cleanup succeeded", True)
except Exception as e:
    check("Cleanup", False, str(e))

# ─────────────────────────────────────────────
section("15. SUMMARY")
# ─────────────────────────────────────────────
passed = sum(1 for _, ok in results if ok)
failed = sum(1 for _, ok in results if not ok)
print(f"\n{passed} passed, {failed} failed out of {len(results)} tests")
if failed:
    print("\nFailed tests:")
    for name, ok in results:
        if not ok:
            print(f"  - {name}")
