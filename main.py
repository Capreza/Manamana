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
    print(owner['id'][0])

    # print("4. Try adding owner with ID 1 and name Gal")
    # illegal_owner = Owner(1, 'Gal')
    # add_owner(illegal_owner)

    print("5. Add apartment")
    apartment = Apartment(123, 'Rothschild 114' , 'Tel Aviv', 'Israel', 100)
    add_apartment(apartment)

