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
        conn.execute(
            """CREATE TABLE Reservations (aid INTEGER NOT NULL, cid INTEGER NOT NULL, start_date DATE, end_date DATE, total_cost FLOAT, PRIMARY KEY(aid,cid,start_date),
            FOREIGN KEY (aid) REFERENCES Apartments(apartment_id),
            FOREIGN KEY (cid) REFERENCES Customers(id));""")
        conn.execute(
            """CREATE TABLE Reviews (aid INTEGER NOT NULL, cid INTEGER NOT NULL, text TEXT, date DATE, rating INTEGER, PRIMARY KEY (aid, cid),FOREIGN KEY (aid) REFERENCES Apartments(apartment_id),
            FOREIGN KEY (cid) REFERENCES Customers(id));""")
        print("created reservations")
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
        conn.execute("DELETE FROM Reservations")
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
        conn.execute("DROP TABLE Reservations CASCADE")
        conn.execute("DROP TABLE Reviews CASCADE")
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
    if Owner.get_owner_id() <= 0:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()  # add owner
        query = sql.SQL("INSERT INTO Owners(id, name) VALUES({id}, {name})").format(
            id=sql.Literal(owner.get_owner_id()),
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
        rows_affected, result = conn.execute(
            "SELECT * FROM Owners WHERE id = " + str(owner_id))
        # rows_affected is the number of rows received by the SELECT
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
    if rows_affected == 0:
        return Owner.bad_owner()
    return Owner(owner_id=result['id'][0], owner_name=result['name'][0])


def delete_owner(owner_id: int) -> ReturnValue:
    conn = None
    if Owner.get_owner_id() <= 0:
        return ReturnValue.BAD_PARAMS
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
    if apartment.get_id()<=0 or apartment.get_size()<=0:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()  # add owner
        query = sql.SQL("""INSERT INTO Apartments(apartment_id, address, city, country, size)
                        VALUES({id}, {address}, {city}, {country}, {size})""").format(
            id=sql.Literal(apartment.get_id()),
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



def get_apartment(apartment_id: int) -> Apartment:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute(
            "SELECT * FROM Apartments WHERE apartment_id = " + str(apartment_id))
        # rows_affected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Apartment.bad_apartment()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Apartment.bad_apartment()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Apartment.bad_apartment()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Apartment.bad_apartment()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Apartment.bad_apartment()
    except Exception as e:
        print(e)
        return Apartment.bad_apartment()
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        return Apartment.bad_apartment()
    return Apartment(id=result['apartment_id'][0], address=result['address'][0], city=result['city'][0],
                     country=result['country'][0], size=result['size'][0])


def delete_apartment(apartment_id: int) -> ReturnValue:
    if apartment_id<=0:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_affected, _ = conn.execute(
            "DELETE FROM Apartments WHERE apartment_id = " + str(apartment_id))
        # rows_affected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    if customer.get_customer_id()<=0:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        id = sql.Literal(customer.get_customer_id())
        name = sql.Literal(customer.get_customer_name())
        query = sql.SQL("INSERT INTO Customers(id, name) VALUES({id}, {name})").format(
            id=id, name=name)
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


def get_customer(customer_id: int) -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute(
            "SELECT * FROM Customers WHERE id = " + str(customer_id))
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Customer.bad_customer()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Customer.bad_customer()
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Customer.bad_customer()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Customer.bad_customer()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Customer.bad_customer()
    except Exception as e:
        print(e)
        return Customer.bad_customer()
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        return Customer.bad_customer()
    id = result['id'][0]
    name = result['name'][0]
    return Customer(id, name)


def delete_customer(customer_id: int) -> ReturnValue:
    if customer_id<=0:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customers WHERE id={0}").format(
            sql.Literal(customer_id))
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


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_cost: float,
                              conn=None):
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
        INSERT INTO Reservations(aid, cid, start_date, end_date, total_cost)
        SELECT {apartment_id}, {customer_id}, {start_date}, {end_date}, {total_cost}
        WHERE NOT EXISTS (
            SELECT 1 FROM Reservations
            WHERE aid = {apartment_id}
            AND NOT(
                {start_date} > end_date OR
                {end_date} < start_date
            )
        )
        """).format(
            apartment_id=sql.Literal(apartment_id),
            customer_id=sql.Literal(customer_id),
            start_date=sql.Literal(start_date.strftime('%Y-%m-%d')),
            end_date=sql.Literal(end_date.strftime('%Y-%m-%d')),
            total_cost=sql.Literal(total_cost)
        )
        rows_affected, _ = conn.execute(query)

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
        print('found foreign key violation')
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        # this means that the reservation was not made but no DB error raised
        # so the reason is conflict with another reservation
        return ReturnValue.BAD_PARAMS
    return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
        DELETE * FROM Reservations WHERE cid = {customer_id} AND aid = {apartment_id} AND start_date = {start_date}
            )
        )
        """).format(
            apartment_id=sql.Literal(apartment_id),
            customer_id=sql.Literal(customer_id),
            start_date=sql.Literal(start_date.strftime('%Y-%m-%d')),
        )
        rows_affected, _ = conn.execute(query)

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
        print('found foreign key violation')
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        # this means that the reservation was not made but no DB error raised
        # so the reason is conflict with another reservation
        return ReturnValue.BAD_PARAMS
    return ReturnValue.OK


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    # TODO: implement
    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
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
