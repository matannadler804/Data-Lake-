# -*- coding: utf-8 -*-

import random
import time

class DataLake:
    def __init__(self, num_sources=10, num_users=50):
        self.num_sources = num_sources
        self.num_users = num_users
        self.sources = []
        self.users = []
        self.data_storage = []
        self.characterize_data_lake()
        self.construct_data_lake()
        self.operate_and_maintain_data_lake()

    def characterize_data_lake(self):
        print("Characterizing the Data Lake...")
        self.sources = [{'id': i, 'type': random.choice(['structured', 'unstructured'])} for i in range(1, self.num_sources + 1)]
        self.users = [{'id': i, 'name': f'User_{i}'} for i in range(1, self.num_users + 1)]
        time.sleep(1)
        print(f"Data Lake characterized with {self.num_sources} sources and {self.num_users} users.")

    def construct_data_lake(self):
        print("Constructing the Data Lake...")
        for source in self.sources:
            data_type = source['type']
            data = self.generate_data(data_type)
            self.data_storage.append({'source_id': source['id'], 'data': data, 'type': data_type})
        time.sleep(1)
        print("Data Lake construction complete.")

    def generate_data(self, data_type):
        if data_type == 'structured':
            return {f'field_{i}': random.randint(1, 100) for i in range(5)}
        else:
            return f"Unstructured data blob {random.randint(1, 1000)}"

    def operate_and_maintain_data_lake(self):
        print("Operating and maintaining the Data Lake...")
        for _ in range(5):
            user = random.choice(self.users)
            data_source = random.choice(self.data_storage)
            print(f"User {user['id']} accessing data from source {data_source['source_id']} of type {data_source['type']}")
            time.sleep(0.5)
        print("Data Lake operation and maintenance ongoing.")

    def provide_services(self):
        print("Providing services such as data analysis and processing...")
        # Simulate data analysis
        for data in self.data_storage:
            print(f"Analyzing data from source {data['source_id']}: {data['data']}")
            time.sleep(0.5)
        print("Data analysis and processing complete.")

# Instantiate and run the Data Lake simulation
data_lake = DataLake()
data_lake.provide_services()
