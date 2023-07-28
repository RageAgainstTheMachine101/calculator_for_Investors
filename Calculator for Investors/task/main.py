from sqlalchemy import (
    Column, String, Float, create_engine, ForeignKey, desc, func
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import (
    sessionmaker, declarative_base, relationship, joinedload
)
from sqlite3 import connect
from pandas import read_csv


ROOT_DIR = 'data'
Base = declarative_base()
INFO = '''
P/E = Market price / Net profit
P/S = Market price / Sales
P/B = Market price / Assets
ND/EBITDA = Net debt / EBITDA
ROE = Net profit / Equity
ROA = Net profit / Assets
L/A = Liabilities / Assets
'''


class Companies(Base):
    __tablename__ = 'companies'

    ticker = Column(String, primary_key=True)
    name = Column(String)
    sector = Column(String)

    financial = relationship(
        'Financial',
        uselist=False,
        back_populates="company",
        cascade="all,delete"
    )


class Financial(Base):
    __tablename__ = 'financial'

    ticker = Column(String, ForeignKey(Companies.ticker), primary_key=True)
    ebitda = Column(Float)
    sales = Column(Float)
    net_profit = Column(Float)
    market_price = Column(Float)
    net_debt = Column(Float)
    assets = Column(Float)
    equity = Column(Float)
    cash_equivalents = Column(Float)
    liabilities = Column(Float)

    company = relationship("Companies", back_populates="financial")


class User:
    def __init__(self, option=None):
        self.option = option

    def choose_option(self):
        self.option = int(
            input("Enter an option:\n")
        )


class Menu:
    def __init__(self, name, options):
        self.name = name
        self.options = options

    def __str__(self):
        return f'{self.name}\n' + '\n'.join(
            f'{key} {value}' for key, value in self.options.items()
        ) + '\n'


def calculate_ratio(a, b):
    try:
        result = round(a / b, 2)
    except (ZeroDivisionError, TypeError):
        result = None
    return result


def select_company(companies: list[Companies]) -> (None, Companies):
    if not companies:
        print("Company not found!")
        return

    for i in range(len(companies)):
        print(f"{i} {companies[i].name}")

    ind = int(input("Enter company number:\n"))
    value = companies[ind]

    return value


def set_financial_data() -> Financial:
    ebitda = input("Enter ebitda (in the format '987654321'):\n")
    sales = input("Enter sales (in the format '987654321'):\n")
    net_profit = input("Enter net profit (in the format '987654321'):\n")
    market_price = input(
        "Enter market price (in the format '987654321'):\n"
    )
    net_debt = input("Enter net debt (in the format '987654321'):\n")
    assets = input("Enter assets (in the format '987654321'):\n")
    equity = input("Enter equity (in the format '987654321'):\n")
    cash_equivalents = input(
        "Enter cash equivalents (in the format '987654321'):\n"
    )
    liabilities = input("Enter liabilities (in the format '987654321'):\n")

    financial = Financial(
        ebitda=ebitda,
        sales=sales,
        net_profit=net_profit,
        market_price=market_price,
        net_debt=net_debt,
        assets=assets,
        equity=equity,
        cash_equivalents=cash_equivalents,
        liabilities=liabilities
    )

    return financial


class Calculator:
    def __init__(self, user: User):
        self.main_menu = main_menu
        self.crud_menu = crud_menu
        self.top_ten_menu = top_ten_menu
        self.current_menu = self.main_menu
        self.user = user
        self.current_value = None
        self.companies = None
        self.financial = None
        self.engine = None
        self.session = None

    def set_current_value(self, value: int):
        self.current_value = self.current_menu.options[value]

    def set_current_menu(self, menu: Menu):
        self.current_menu = menu

    def set_up_db(self):
        def read_data():
            self.companies = read_csv(f'{ROOT_DIR}/companies.csv')
            self.financial = read_csv(f'{ROOT_DIR}/financial.csv')

        def create_db():
            # create sqlite3 database 'investor.db'
            with connect('investor.db') as conn:
                conn.cursor()

            # connect to sqlite3 database 'investor.db' via sqlalchemy
            self.engine = create_engine(f'sqlite:///investor.db')
            Base.metadata.create_all(self.engine)

        def insert_data():
            Session = sessionmaker(bind=self.engine)
            self.session = Session()

            # insert data into table companies
            for index, row in self.companies.iterrows():
                company = Companies(
                    ticker=row['ticker'],
                    name=row['name'],
                    sector=row['sector']
                )
                self.session.add(company)

            # insert data into table financial
            for index, row in self.financial.iterrows():
                financial = Financial(
                    ticker=row['ticker'],
                    ebitda=row['ebitda'],
                    sales=row['sales'],
                    net_profit=row['net_profit'],
                    market_price=row['market_price'],
                    net_debt=row['net_debt'],
                    assets=row['assets'],
                    equity=row['equity'],
                    cash_equivalents=row['cash_equivalents'],
                    liabilities=row['liabilities']
                )
                self.session.add(financial)

            self.session.commit()

        try:
            read_data()
        except Exception as e:
            print("Error reading data!")
            print(e)
        else:
            try:
                create_db()
            except Exception as e:
                print("Error creating database!")
                print(e)
            else:
                try:
                    insert_data()
                except IntegrityError:
                    self.session.rollback()
                else:
                    print("Database created successfully!")

    def create_company(self):
        ticker = input("Enter ticker (in the format 'MOON'):\n")
        name = input("Enter company (in the format 'Moon Corp'):\n")
        sector = input("Enter industries (in the format 'Technology'):\n")

        company = Companies(
            ticker=ticker,
            name=name,
            sector=sector
        )
        financial = set_financial_data()
        financial.ticker = ticker

        self.session.add(company)
        self.session.add(financial)
        self.session.commit()

        print("Company created successfully!")

    def read_company(self):
        name = input("Enter company name:\n")
        companies = self.session.query(
            Companies
        ).filter(
            Companies.name.like(f'%{name}%')
        ).options(
            joinedload(Companies.financial)
        ).all()

        company = select_company(companies)

        if company is None:
            return

        ticker = company.ticker
        name = company.name
        sector = company.sector
        pe = calculate_ratio(
            company.financial.market_price, company.financial.net_profit
        )
        ps = calculate_ratio(
            company.financial.market_price, company.financial.sales
        )
        pb = calculate_ratio(
            company.financial.market_price, company.financial.assets
        )
        nd_ebitda = calculate_ratio(
            company.financial.net_debt, company.financial.ebitda
        )
        roe = calculate_ratio(
            company.financial.net_profit, company.financial.equity
        )
        roa = calculate_ratio(
            company.financial.net_profit, company.financial.assets
        )
        la = calculate_ratio(
            company.financial.liabilities, company.financial.assets
        )

        print(
            f'{ticker} {name}\nP/E = {pe}\nP/S = {ps}\nP/B = {pb}'
            f'\nND/EBITDA = {nd_ebitda}\nROE = {roe}\nROA = {roa}\nL/A = {la}'
        )

    def update_company(self):
        name = input("Enter company name:\n")
        companies = self.session.query(
            Companies
        ).filter(
            Companies.name.like(f'%{name}%')
        ).options(
            joinedload(Companies.financial)
        ).all()

        company = select_company(companies)

        if company is None:
            return

        financial = self.session.query(Financial).filter(
            Financial.ticker == company.ticker
        ).first()

        financial_ = set_financial_data()
        financial.ebitda = financial_.ebitda
        financial.sales = financial_.sales
        financial.net_profit = financial_.net_profit
        financial.market_price = financial_.market_price
        financial.net_debt = financial_.net_debt
        financial.assets = financial_.assets
        financial.equity = financial_.equity
        financial.cash_equivalents = financial_.cash_equivalents
        financial.liabilities = financial_.liabilities

        self.session.commit()

        print("Company updated successfully!")

    def delete_company(self):
        name = input("Enter company name:\n")
        companies = self.session.query(
            Companies
        ).filter(
            Companies.name.like(f'%{name}%')
        ).options(
            joinedload(Companies.financial)
        ).all()

        company = select_company(companies)

        if company is None:
            return

        self.session.delete(company)
        self.session.commit()

        print("Company deleted successfully!")

    def list_companies(self):
        companies = self.session.query(
            Companies
        ).order_by(
            Companies.ticker
        ).all()

        print('COMPANY LIST')

        for company in companies:
            print(company.ticker, company.name, company.sector)

    def top_ten(self, metric):
        metrics = {
            'List by ND/EBITDA': Financial.net_debt
            / Financial.ebitda,
            'List by ROE': Financial.net_profit
            / Financial.equity,
            'List by ROA': Financial.net_profit
            / Financial.assets
        }

        companies = self.session.query(
            Companies, metrics[metric].label(metric)
        ).join(
            Financial
        ).order_by(
            desc(metrics[metric])
        ).limit(10).all()

        print(f'TICKER {metric.split()[-1]}')

        for company, metric_value in companies:
            print(company.ticker, round(metric_value, 2))

    def run(self):
        print("Welcome to the Investor Program!\n")

        while True:
            print(self.current_menu)

            try:
                self.user.choose_option()
                self.set_current_value(self.user.option)
            except (ValueError, KeyError):
                print("Invalid option!\n")
                self.set_current_menu(main_menu)
            else:
                if self.current_value == 'Exit':
                    print("\nHave a nice day!")
                    exit()
                elif self.current_value == 'CRUD operations':
                    self.set_current_menu(self.crud_menu)
                elif self.current_value == 'Show top ten companies by criteria':
                    self.set_current_menu(self.top_ten_menu)
                elif self.current_value == 'Back':
                    self.set_current_menu(main_menu)
                elif self.current_value == 'Create a company':
                    self.create_company()
                    self.set_current_menu(main_menu)
                elif self.current_value == 'Read a company':
                    self.read_company()
                    self.set_current_menu(main_menu)
                elif self.current_value == 'Update a company':
                    self.update_company()
                    self.set_current_menu(main_menu)
                elif self.current_value == 'Delete a company':
                    self.delete_company()
                    self.set_current_menu(main_menu)
                elif self.current_value == 'List all companies':
                    self.list_companies()
                    self.set_current_menu(main_menu)
                elif self.current_value in [
                    'List by ND/EBITDA', 'List by ROE', 'List by ROA',
                ]:
                    self.top_ten(self.current_value)
                    self.set_current_menu(main_menu)
                else:
                    print('Not implemented!')
                    self.set_current_menu(main_menu)
            finally:
                print()


main_menu = Menu(
    name='MAIN MENU',
    options={
        0: 'Exit',
        1: 'CRUD operations',
        2: 'Show top ten companies by criteria'
    }
)
crud_menu = Menu(
    name='CRUD MENU',
    options={
        0: 'Back',
        1: 'Create a company',
        2: 'Read a company',
        3: 'Update a company',
        4: 'Delete a company',
        5: 'List all companies'
    }
)
top_ten_menu = Menu(
    name='TOP TEN MENU',
    options={
        0: 'Back',
        1: 'List by ND/EBITDA',
        2: 'List by ROE',
        3: 'List by ROA'
    }
)

person = User()
calculator = Calculator(person)

calculator.set_up_db()
calculator.run()
