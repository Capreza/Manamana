from typing import List, Tuple
from psycopg2 import sql # type: ignore
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
        conn.execute(
            """CREATE TABLE Owns (oid INTEGER NOT NULL, aid INTEGER NOT NULL, PRIMARY KEY (aid, oid),
            FOREIGN KEY (aid) REFERENCES Apartments(apartment_id),
            FOREIGN KEY (oid) REFERENCES Owners(id));""")
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
        conn.execute("DROP TABLE Owners CASCADE")
        conn.execute("DROP TABLE Owns CASCADE")
        conn.execute("DROP TABLE Apartments CASCADE")
        conn.execute("DROP TABLE Customers CASCADE")
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
    if owner.get_owner_id() is None or owner.get_owner_id() <= 0:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
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
    if owner_id is None or owner_id <= 0:
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
    if apartment.get_id() <= 0 or apartment.get_size() <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    try:
        conn = Connector.DBConnector()
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
    if apartment_id <= 0:
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
    if customer.get_customer_id() is None or customer.get_customer_id() <= 0:
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
    if customer_id <= 0:
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
    if customer_id is None or apartment_id is None or total_cost is None or start_date is None or end_date is None:
        return ReturnValue.BAD_PARAMS

    if customer_id <= 0 or apartment_id <= 0 or total_cost <= 0 or end_date <= start_date:
        return ReturnValue.BAD_PARAMS

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
        INSERT INTO Reservations(aid, cid, start_date, end_date, total_cost)
        SELECT {apartment_id}, {customer_id}, {start_date}, {end_date}, {total_cost}
        WHERE NOT EXISTS (
            SELECT 1 FROM Reservations
            WHERE aid = {apartment_id}
            AND NOT(
                {start_date} >= end_date OR
                {end_date} <= start_date
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
    if customer_id is None or apartment_id is None or start_date is None:
        return ReturnValue.BAD_PARAMS

    if customer_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
        DELETE FROM Reservations WHERE cid = {customer_id} AND aid = {apartment_id} AND start_date = {start_date}
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
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        # this means that the reservation did not exist
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    if customer_id is None or apartment_id is None or review_date is None or rating is None or review_text is None:
        return ReturnValue.BAD_PARAMS

    if customer_id <= 0 or apartment_id <= 0 or 0 >= rating or rating > 10:
        return ReturnValue.BAD_PARAMS

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
        INSERT INTO Reviews(aid, cid, text, date, rating)
        SELECT {apartment_id}, {customer_id}, {text}, {date}, {rating}
        WHERE EXISTS (
            SELECT 1 FROM Reservations
            WHERE aid = {apartment_id} AND cid = {customer_id}
            AND {date} >= end_date
        )
        """).format(
            apartment_id=sql.Literal(apartment_id),
            customer_id=sql.Literal(customer_id),
            date=sql.Literal(review_date.strftime('%Y-%m-%d')),
            rating=sql.Literal(rating),
            text=sql.Literal(review_text)
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
        return ReturnValue.ALREADY_EXISTS  # not sure about this
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
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
    if customer_id is None or apartment_id is None or update_date is None or new_rating is None or new_text is None:
        return ReturnValue.BAD_PARAMS

    if customer_id <= 0 or apartment_id <= 0 or 0 >= new_rating or new_rating > 10:
        return ReturnValue.BAD_PARAMS

    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
        UPDATE Reviews
        SET text={new_text}, date={update_date}, rating={new_rating}
        WHERE aid={apartment_id} AND cid={customer_id} AND date <= {update_date}
        """).format(
            apartment_id=sql.Literal(apartment_id),
            customer_id=sql.Literal(customer_id),
            update_date=sql.Literal(update_date.strftime('%Y-%m-%d')),
            new_rating=sql.Literal(new_rating),
            new_text=sql.Literal(new_text)
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
        return ReturnValue.ALREADY_EXISTS  # not sure about this
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
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    if owner_id is None or apartment_id is None:
        return ReturnValue.BAD_PARAMS
    if owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""INSERT INTO Owns(oid, aid)
                        SELECT {oid}, {aid}
                        WHERE NOT EXISTS(
                        SELECT 1 FROM Owns
                        WHERE aid = {aid}
                        )
                        """).format(
            oid=sql.Literal(owner_id),
            aid=sql.Literal(apartment_id))
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
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        return ReturnValue.ALREADY_EXISTS
    return ReturnValue.OK


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    if owner_id is None or apartment_id is None:
        return ReturnValue.BAD_PARAMS
    if owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    try:
        conn = Connector.DBConnector()
        "DELETE FROM Owners WHERE id={0}"
        query = sql.SQL(f"""DELETE FROM Owns
                        WHERE {owner_id} = oid AND {apartment_id} = aid

                        """).format(
            owner_id=sql.Literal(owner_id),
            apartment_id=sql.Literal(apartment_id))
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
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ReturnValue.OK


def get_apartment_owner(apartment_id: int) -> Owner:
    conn = None
    if not apartment_id:
        return Owner.bad_owner()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            SELECT OWNERS.id, OWNERS.name
                            FROM OWNERS, OWNS
                            WHERE OWNERS.id = OWNS.oid AND {aid} = OWNS.aid
                            """).format(
            aid=sql.Literal(apartment_id))
        rows_affected, result = conn.execute(query)

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
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        return Owner.bad_owner()
    owner_name = result['name'][0]
    owner_id = result['id'][0]
    return Owner(owner_name=owner_name, owner_id=owner_id)


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = None
    if not owner_id or owner_id <= 0:
        return []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            SELECT *
                            FROM OWNS, Apartments
                            WHERE Owns.oid = {owner_id} AND Apartments.apartment_id = OWNS.aid
                            """).format(
            owner_id=sql.Literal(owner_id))
        rows_affected, result = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    if rows_affected == 0:
        return []
    apartment_ids = result['apartment_id']
    apartment_citys = result['city']
    apartment_country = result['country']
    apartment_addresses = result['address']
    apartment_sizes = result['size']
    return [Apartment(id=apartment_ids[i], address=apartment_addresses[i], city=apartment_citys[i], country=apartment_country[i], size=apartment_sizes[i]) for i in range(len(apartment_ids))]


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    conn = None
    try:

        conn = Connector.DBConnector()
        query = sql.SQL("""
                            CREATE VIEW ApartmentRatings AS
                            SELECT apartment_id, COALESCE(AVG(rating),0) rating
                            FROM (
                            SELECT * FROM Apartments LEFT OUTER JOIN Reviews
                                ON Apartments.apartment_id = Reviews.aid)
                            GROUP BY apartment_id;
                            """)
        rows_affected, result = conn.execute(query)  # type: ignore
        query = sql.SQL("""
                            SELECT rating
                            FROM ApartmentRatings
                            WHERE apartment_id = {aid}
                            """).format(
            aid=sql.Literal(apartment_id))
        rows_affected, result = conn.execute(query)

        query = sql.SQL("""
                            DROP VIEW ApartmentRatings
                            """)
        _, _ = conn.execute(query)  # type: ignore
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return 0
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return 0
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return 0
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return 0
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return 0
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    return result['rating'][0]


def get_owner_rating(owner_id: int) -> float:
    conn = None
    try:

        conn = Connector.DBConnector()
        query = sql.SQL("""
                            CREATE VIEW ApartmentRatings AS
                            SELECT apartment_id, COALESCE(AVG(rating),0) rating
                            FROM (
                            SELECT * FROM Apartments LEFT OUTER JOIN Reviews
                                ON Apartments.apartment_id = Reviews.aid)
                            GROUP BY apartment_id;
                            """)
        conn.execute(query)  # type: ignore

        # query = sql.SQL("""
        #                     CREATE VIEW OwnerApartments{oid} AS
        #                     SELECT *
        #                     FROM Owns
        #                     WHERE oid = {oid}
        #                     """).format(
        #     oid=sql.Literal(owner_id))
        # rows_affected, result = conn.execute(query)

        query = sql.SQL("""
                            SELECT COALESCE(AVG(ApartmentRatings.rating),0) rating
                            FROM Owns,ApartmentRatings
                            WHERE ApartmentRatings.apartment_id = Owns.aid AND Owns.oid = {oid}
                            """).format(
            oid=sql.Literal(owner_id))
        rows_affected, result = conn.execute(query)

        # query = sql.SQL("""
        #                             DROP VIEW OwnerApartments{oid}
        #                             """).format(
        #     oid=sql.Literal(owner_id))
        #
        # _, _ = conn.execute(query)

        query = sql.SQL("""
                            DROP VIEW ApartmentRatings
                            """)
        _, _ = conn.execute(query)  # type: ignore

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return 0
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return 0
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return 0
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return 0
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return 0
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    return result['rating'][0]


def get_top_customer() -> Customer:
    conn = None
    try:

        conn = Connector.DBConnector()
        query = sql.SQL("""
                            CREATE VIEW TopCustomers AS
                            SELECT id, COALESCE(COUNT(cid),0) num_res
                            FROM (SELECT * FROM Customers LEFT JOIN Reservations ON Customers.id = Reservations.cid)
                            GROUP BY id
                            """)
        _, _ = conn.execute(query)  # type: ignore

        query = sql.SQL(""" SELECT *
                            FROM Customers
                            WHERE id = (
                            SELECT id
                            FROM TopCustomers
                            ORDER BY num_res DESC, id ASC
                            LIMIT 1)
                            """)
        rows_affected, result = conn.execute(query)  # type: ignore

        query = sql.SQL("""
                                    DROP VIEW TopCustomers
                                    """)
        _, _ = conn.execute(query)  # type: ignore

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
    finally:
        conn.close()  # type: ignore

    id = result['id'][0]
    name = result['name'][0]
    return Customer(id, name)


def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = None
    try:

        conn = Connector.DBConnector()
        query = sql.SQL("""
                                    CREATE VIEW Ownership AS
                                    SELECT Owners.id, Owners.name, Owns.aid
                                            FROM Owners LEFT OUTER JOIN Owns
                                            ON Owners.id = Owns.oid
                                    """)
        conn.execute(query)  # type: ignore

        query = sql.SQL("""
                                    CREATE VIEW ReservationsByOwner AS
                                    SELECT Ownership.id, Ownership.name, Reservations.aid
                                    FROM Ownership
                                    LEFT OUTER JOIN Reservations
                                    ON Ownership.aid = Reservations.aid
                                    """)
        conn.execute(query)  # type: ignore

        query = sql.SQL("""
                            SELECT name, Count(aid)
                            FROM ReservationsByOwner
                            GROUP BY id, name
                            """)
        rows_affected, result = conn.execute(query)  # type: ignore

        query = sql.SQL("""
                                    DROP VIEW ReservationsByOwner
                                    """)
        conn.execute(query)  # type: ignore

        query = sql.SQL("""
                                    DROP VIEW Ownership
                                    """)
        conn.execute(query)  # type: ignore
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore
    return [(result['name'][i], result['count'][i]) for i in range(len(result['name']))]


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                SELECT Owners.id, Owners.name
                FROM Owners, Owns, Apartments
                WHERE Owners.id = Owns.oid and Owns.aid = Apartments.apartment_id
                GROUP BY Owners.id
                HAVING COUNT(DISTINCT (Apartments.city,Apartments.country)) = (SELECT COUNT(DISTINCT (city,country)) FROM Apartments)
                            """)
        rows_affected, result = conn.execute(query)  # type: ignore

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore

    return [Owner(result['id'][i], result['name'][i]) for i in range(len(result['id']))]


def best_value_for_money() -> Apartment:
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        CREATE VIEW NightlyPrices AS
                        SELECT
                            aid,
                            AVG(total_cost/(end_date-start_date)) AS avg_nightly_price
                        FROM Reservations
                        GROUP BY aid
                        """)
        conn.execute(query)  # type: ignore

        conn = Connector.DBConnector()
        query = sql.SQL("""
                            CREATE VIEW ApartmentRatings AS
                            SELECT apartment_id, COALESCE(AVG(rating),0) rating
                            FROM (
                            SELECT * FROM Apartments LEFT OUTER JOIN Reviews
                                ON Apartments.apartment_id = Reviews.aid)
                            GROUP BY apartment_id;
                            """)
        conn.execute(query)  # type: ignore

        conn = Connector.DBConnector()
        query = sql.SQL("""
                            SELECT Apartments.apartment_id, Apartments.address, Apartments.city, Apartments.country, Apartments.size
                            FROM Apartments, NightlyPrices, ApartmentRatings
                            WHERE Apartments.apartment_id = NightlyPrices.aid AND Apartments.apartment_id = ApartmentRatings.aid
                            ORDER BY avg_nightly_price/rating DESC
                            LIMIT 1
                            """)
        rows_affected, result = conn.execute(query)  # type: ignore

        query = sql.SQL("""
                            DROP VIEW NightlyPrices, ApartmentRatings
                            """)
        conn.execute(query)  # type: ignore

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
    finally:
        conn.close()  # type: ignore

    if rows_affected == 0:
        return Apartment.bad_apartment()

    return Apartment(id=result['apartment_id'][0], address=result['address'][0], city=result['city'][0],
                     country=result['country'][0], size=result['size'][0])


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    try:
        # reservations by month in given year

        conn = Connector.DBConnector()
        query = sql.SQL("""
                SELECT generate_series as month, COALESCE(SUM(total_cost)*0.15,0) as total_profit FROM(
                (SELECT * FROM GENERATE_SERIES(1,12,1)) months LEFT OUTER JOIN (
                SELECT extract(month from end_date) as month, total_cost
                FROM Reservations
                WHERE extract(year from end_date) = {year}
                ) res_by_month
                ON months.generate_series = res_by_month.month) GROUP BY generate_series;
                            """.format(year=sql.Literal(year)))
        rows_affected, result = conn.execute(query)  # type: ignore

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
    finally:
        conn.close()  # type: ignore

    return [(result['month'][i], result['total_profit'][i]) for i in range(len(result['month']))]


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    return []
