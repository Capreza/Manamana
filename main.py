from Solution import *

if __name__ == '__main__':
    print("0. Creating all tables")
    create_tables()

    print("1. Add owner with ID 1 and name Roei")
    owner1 = Owner(1, 'Roei')
    add_owner(owner1)

    print("2. add owner with ID 2 and name Noa")
    owner2 = Owner(2, 'Noa')
    add_owner(owner2)

    print("3. Get owner with ID 1")
    owner = get_owner(1)
    print(owner)

    # print("4. Try adding owner with ID 1 and name Gal")
    # illegal_owner = Owner(1, 'Gal')
    # add_owner(illegal_owner)

    print("5. Add apartment")
    apartment = Apartment(123, 'Rothschild 114' , 'Tel Aviv', 'Israel', 100)
    add_apartment(apartment)

    print("6. Get apartment with ID 123")
    fetched_apartment = get_apartment(123)
    print(fetched_apartment)

    print("7. delete apartment with ID 123")
    fetched_apartment = delete_apartment(123)
    print(fetched_apartment)

    print("8. Add customer")
    cust = Customer(1234, 'itzik')
    add_customer(cust)

    print("9. Get customer with ID 1234")
    customer = get_customer(1234)
    print(customer)

    print("10. delete customer with ID 1234")
    customer = delete_customer(1234)
    print(customer)

    print("11. add reservation 2.3-5.3")
    reservation = customer_made_reservation(1234,123,datetime.today(),datetime.today().replace(day=5),total_cost=69)
    print(reservation)


    print("12. add reservation 2.3-4.3")
    reservation = customer_made_reservation(1234,123,datetime.today(),datetime.today().replace(day=4),total_cost=69)
    print(reservation)

    print('delete all tables')
    drop_tables()


