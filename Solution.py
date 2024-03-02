from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def create_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(
            "CREATE TABLE Owners(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        conn.execute("""CREATE TABLE Apartments (
            apartment_id INTEGER PRIMARY KEY,
            city TEXT NOT NULL,
            country TEXT NOT NULL,
            address TEXT NOT NULL,
            size INTEGER NOT NULL,
            UNIQUE (city, country, address)
        );""")
        conn.execute(
            "CREATE TABLE Customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore


def clear_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Owners")
        conn.execute("DELETE FROM Apartments")
        conn.execute("DELETE FROM Customers")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore


def drop_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE Owners")
        conn.execute("DROP TABLE Apartments")
        conn.execute("DROP TABLE Customers")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore


def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()        # add owner
        query = sql.SQL("INSERT INTO Owners(id, name) VALUES({id}, {name})").format(id=sql.Literal(owner.get_owner_id()),
                                                                                    name=sql.Literal(owner.get_owner_name()))
        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
        return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(
            "SELECT * FROM Owners WHERE id = " + str(owner_id))
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Owner.bad_owner()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Owner.bad_owner()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Owner.bad_owner()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Owner.bad_owner()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Owner.bad_owner()
    except Exception as e:
        print(e)
        return Owner.bad_owner()
    finally:
        conn.close()  # type: ignore
        return result


def delete_owner(owner_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owners WHERE id={0}").format(
            sql.Literal(owner_id))
        rows_affected, _ = conn.execute(query)
        if rows_affected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
        return ReturnValue.OK


def add_apartment(apartment: Apartment) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()        # add owner
        query = sql.SQL("""INSERT INTO Apartment(apartment_id, address, city, country, size)
                        VALUES({id}, {name})""").format(id=sql.Literal(apartment.get_id()),
                                                        address=sql.Literal(
            apartment.get_address()),
            city=sql.Literal(
            apartment.get_city()),
            country=sql.Literal(
            apartment.get_country()),
            size=sql.Literal(apartment.get_size()))

        conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
        return ReturnValue.OK
    pass


def get_apartment(apartment_id: int) -> Apartment:
    # TODO: implement
    pass


def delete_apartment(apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_customer(customer: Customer) -> ReturnValue:
    # TODO: implement
    pass


def get_customer(customer_id: int) -> Customer:
    # TODO: implement
    pass


def delete_customer(customer_id: int) -> ReturnValue:
    # TODO: implement
    pass


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_price: float) -> ReturnValue:
    # TODO: implement
    pass


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    # TODO: implement
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    # TODO: implement
    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    # TODO: implement
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    pass


def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass


def get_top_customer() -> Customer:
    # TODO: implement
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass
