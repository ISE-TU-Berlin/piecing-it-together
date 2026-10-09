#!/usr/bin/python
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import random
from locust import FastHttpUser, TaskSet, between #type: ignore
import datetime

random.seed(1337)

states_abbr = (
        'AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FM', 'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN',
        'IA',
        'KS', 'KY', 'LA', 'ME', 'MH', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY',
        'NC',
        'ND', 'MP', 'OH', 'OK', 'OR', 'PW', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VI', 'VA', 'WA',
        'WV',
        'WI', 'WY')

safe_users = [
    "admin", "administrator", "root", "user", "test", "guest", "info", "adm", "mysql", "user1", "oracle", "ftp",
    "pi", "puppet", "ansible", "ec2-user"
]

class Faker():
    
    def _random_string(self, length):
        letters = 'abcdefghijklmnopqrstuvwxyz'
        return ''.join(random.choice(letters) for i in range(length))
    
    def _random_digits(self, length):
        digits = '0123456789'
        return ''.join(random.choice(digits) for i in range(length))
    
    def email(self):
        domains = ['example.com', 'test.com', 'demo.com']
        return f"{random.choice(safe_users)}@{random.choice(domains)}"

    def street_address(self):
        return f"{random.randint(1,9999)} {self._random_string(10)} Street"

    def zipcode(self):
        return "%05d" % random.randint(501, 99950)

    city_prefixes = ('North', 'East', 'West', 'South', 'New', 'Lake', 'Port')
    
    def city(self):
        return f"{random.choice(self.city_prefixes)} {self._random_string(10)}"

    def state_abbr(self):
        return random.choice(states_abbr)

    def country(self):
        return "Luxembourg"

    def credit_card_number(self, card_type="visa"):
        return self._generate_number("4", 16)

    luhn_lookup = {'0': 0, '1': 2, '2': 4, '3': 6, '4': 8,
                   '5': 1, '6': 3, '7': 5, '8': 7, '9': 9}
        
    def _generate_number(self, prefix, length):
        """
        'prefix' is the start of the CC number as a string, any number of digits.
        'length' is the length of the CC number to generate. Typically 13 or 16
        """
        # Generate random char digits
        number = prefix + self._random_digits(length - len(prefix) - 1)
        reverse = number[::-1]
        # Calculate sum
        tot = 0
        pos = 0
        while pos < length - 1:
            tot += self.luhn_lookup[reverse[pos]]
            if pos != (length - 2):
                tot += int(reverse[pos+1])
            pos += 2
        # Calculate check digit
        check_digit = (10 - (tot % 10)) % 10
        number += str(check_digit)
        return number

fake = Faker()

products = [
    '0PUK6V6EV0',
    '1YMWWN1N4O',
    '2ZYFJ3GM2N',
    '66VCHSJNUP',
    '6E92ZMYYFZ',
    '9SIQT8TOJO',
    'L9ECAV7KIM',
    'LS4PSXUNUM',
    'OLJCESPC7Z']

def index(l):
    l.client.get("/")

def setCurrency(l):
    currencies = ['EUR', 'USD', 'JPY', 'CAD', 'GBP', 'TRY']
    l.client.post("/setCurrency",
        {'currency_code': random.choice(currencies)})

def browseProduct(l):
    l.client.get("/product/" + random.choice(products))

def viewCart(l):
    l.client.get("/cart")

def addToCart(l):
    product = random.choice(products)
    l.client.get("/product/" + product)
    l.client.post("/cart", {
        'product_id': product,
        'quantity': random.randint(1,10)})
    
def empty_cart(l):
    l.client.post('/cart/empty')

def checkout(l):
    addToCart(l)
    current_year = datetime.datetime.now().year+1
    l.client.post("/cart/checkout", {
        'email': fake.email(),
        'street_address': fake.street_address(),
        'zip_code': fake.zipcode(),
        'city': fake.city(),
        'state': fake.state_abbr(),
        'country': fake.country(),
        'credit_card_number': fake.credit_card_number(card_type="visa"),
        'credit_card_expiration_month': f"{random.randint(6, 9)}",
        'credit_card_expiration_year': f"{random.randint(current_year, current_year + 6)}",
        'credit_card_cvv': f"{random.randint(100, 999)}",
    })
    
def logout(l):
    l.client.get('/logout')  


class UserBehavior(TaskSet):

    def on_start(self):
        index(self)

    tasks = {index: 1,
        setCurrency: 2,
        browseProduct: 10,
        addToCart: 2,
        viewCart: 3,
        checkout: 1}

class WebsiteUser(FastHttpUser):
    tasks = [UserBehavior]
    wait_time = between(1, 10)
