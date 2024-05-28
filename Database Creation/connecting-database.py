import mysql.connector
from mysql.connector import Error
import pandas as pd

##--------------------------------------------------------------------------------------##
## write code to establish a connection to the MySQL server

def create_server_connection(host_name, user_name, user_password):
    connection = None  ##------------ -------------------------------------### this line closes any existing connections so the server doesn't get confused----###
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Databse connection successful")
    except Error as err:
        print(f"Error: '{err}'")
        
    
    return connection

connection = create_server_connection ('host_name', 'user_name', 'user_password')    ####---- the successful connection is going to be put into a variable for ease of use



####----------------------------------------- creating a new database----------------------####

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Databse created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        
create_database_query = "CREATE DATABASE school"
# create_database_query = "CREATE DATABASE work"
create_database(connection, create_database_query)


#####----------------------------------------------------------------------------------------######

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
            
        )
        print(f"You have successfully connected to the '{db_name}' database")
    except Error as err:
        print(f"Error: '{err}'")
        
    return connection

#####----------------------------------------------------------------------------------------######

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
        
#####---------------------------------------------------------------------------------------###### 

create_teacher_table = """
CREATE TABLE teacher (
    teacher_id INT PRIMARY KEY,
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40) NOT NULL,
    language_1 VARCHAR(50) NOT NULL,
    language_2 VARCHAR(50),
    dob DATE,
    tax_id INT UNIQUE,
    phone_no VARCHAR(20)
);
"""

create_client_table = """
CREATE TABLE client (
    client_id INT PRIMARY KEY,
    client_name VARCHAR(200) NOT NULL,
    address VARCHAR(100) NOT NULL,
    industry VARCHAR(40)
);
"""

create_participant_table = """
CREATE TABLE participant (
  participant_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  phone_no VARCHAR(20),
  client INT
);
"""
create_course_table = """
CREATE TABLE course (
  course_id INT PRIMARY KEY,
  course_name VARCHAR(40) NOT NULL,
  language VARCHAR(3) NOT NULL,
  level VARCHAR(2),
  course_length_weeks INT,
  start_date DATE,
  in_school BOOLEAN,
  teacher INT,
  client INT
);
"""

connection = create_db_connection("host","user_name", "user_password", "db_name")
execute_query(connection, create_teacher_table)
execute_query(connection, create_course_table)
execute_query(connection, create_participant_table)
execute_query(connection, create_client_table)

####------------------------------------------------------------------------------------#####


alter_participant = """
ALTER TABLE participant
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

alter_course = """
ALTER TABLE course
ADD FOREIGN KEY(teacher)
REFERENCES teacher(teacher_id)
ON DELETE SET NULL;
"""

alter_course_again = """
ALTER TABLE course
ADD FOREIGN KEY(client)
REFERENCES client(client_id)
ON DELETE SET NULL;
"""

create_takescourse_table = """
CREATE TABLE takes_course (
  participant_id INT,
  course_id INT,
  PRIMARY KEY(participant_id, course_id),
  FOREIGN KEY(participant_id) REFERENCES participant(participant_id) ON DELETE CASCADE,
  FOREIGN KEY(course_id) REFERENCES course(course_id) ON DELETE CASCADE
);
"""

connection = create_db_connection("host","user_name", "user_password", "db_name")
execute_query(connection, alter_participant)
execute_query(connection, alter_course)
execute_query(connection, alter_course_again)
execute_query(connection, create_takescourse_table)

########-----------------------------------------------------------------------------------##########
execute_query(connection, create_takescourse_table)

# clear_teacher_table = "DELETE FROM teacher"
# execute_query(connection, clear_teacher_table)

pop_teacher = """
INSERT INTO teacher VALUES
(1, 'Alice', 'Johnson', 'ENG', 'SPA', '1980-03-15', 11111, '+491231234567'),
(2, 'Bob', 'Williams', 'FRA', NULL, '1975-07-22', 22222, '+491234567890'),
(3, 'Charlie', 'Brown', 'GER', 'ENG', '1988-05-30', 33333, '+447812345678'),
(4, 'Diana', 'Smith', 'ITA', 'FRA', '1992-09-17', 44444, '+491731234567'),
(5, 'Evan', 'Davis', 'ENG', 'GER', '1979-12-01', 55555, '+491741234567'),
(6, 'Fiona', 'Miller', 'SPA', 'ENG', '1986-04-18', 66666, '+491751234567'),
(7, 'George', 'Wilson', 'RUS', 'ENG', '1990-11-23', 77777, '+491761234567'),
(8, 'Hannah', 'Moore', 'JPN', NULL, '1985-06-05', 88888, '+491771234567'),
(9, 'Ian', 'Taylor', 'CHI', 'SPA', '1983-01-14', 99999, '+491781234567'),
(10, 'Jessica', 'Anderson', 'ENG', 'FRA', '1994-10-02', 10101, '+491791234567'),
(11, 'Kyle', 'Thomas', 'SPA', 'ITA', '1982-02-28', 20202, '+491801234567'),
(12, 'Laura', 'Jackson', 'ENG', 'GER', '1991-08-19', 30303, '+491811234567'),
(13, 'Jane', 'Doe', 'FRE', 'ENG', '1999-03-12', 89887, '+809111234567'),
(14, 'John', 'Ham', 'ENG', 'GRE', '1945-03-12', 90508, '+897045640877'),
(15, 'Andrew', 'Scott', 'ENG', 'FRA', '1987-03-22', 12121, '+491821234567'),
(16, 'Brenda', 'Carter', 'SPA', NULL, '1990-07-18', 23232, '+491831234567'),
(17, 'Catherine', 'Evans', 'GER', 'ENG', '1978-11-05', 34343, '+491841234567'),
(18, 'Daniel', 'Roberts', 'ITA', 'SPA', '1983-04-14', 45454, '+491851234567'),
(19, 'Emily', 'Clark', 'ENG', 'RUS', '1995-09-09', 56565, '+491861234567'),
(20, 'Frank', 'Lewis', 'JPN', 'ENG', '1989-06-25', 67676, '+491871234567'),
(21, 'Grace', 'Walker', 'CHI', 'GER', '1982-02-11', 78787, '+491881234567'),
(22, 'Henry', 'Hall', 'FRE', 'ITA', '1976-12-19', 89898, '+491891234567'),
(23, 'Isabella', 'Allen', 'ENG', 'SPA', '1993-05-16', 90909, '+491901234567'),
(24, 'Jack', 'Young', 'RUS', NULL, '1984-08-23', 10110, '+491911234567'),
(25, 'Karen', 'Harris', 'GER', 'FRA', '1992-01-07', 11211, '+491921234567'),
(26, 'Leon', 'King', 'ENG', 'ITA', '1980-10-30', 12312, '+491931234567'),
(27, 'Megan', 'Wright', 'SPA', 'CHI', '1985-03-21', 13413, '+491941234567'),
(28, 'Nathan', 'Lopez', 'FRE', 'GER', '1977-11-19', 14514, '+491951234567'),
(29, 'Olivia', 'Hill', 'ENG', 'FRA', '1991-05-25', 15615, '+491961234567'),
(30, 'Paul', 'Green', 'JPN', 'SPA', '1988-09-10', 16716, '+491971234567'),
(31, 'Quincy', 'Adams', 'CHI', 'RUS', '1982-07-28', 17817, '+491981234567'),
(32, 'Rachel', 'Nelson', 'ENG', 'GER', '1980-04-02', 18918, '+491991234567'),
(33, 'Sam', 'Baker', 'FRA', 'ENG', '1975-12-15', 19019, '+492001234567'),
(34, 'Tina', 'Parker', 'SPA', 'CHI', '1986-08-03', 20120, '+492011234567'),
(35, 'Uma', 'Mitchell', 'GER', 'FRE', '1994-11-22', 21221, '+492021234567'),
(36, 'Victor', 'Perez', 'ITA', 'ENG', '1981-01-13', 22322, '+492031234567'),
(37, 'Wendy', 'Cruz', 'RUS', 'SPA', '1978-10-04', 23423, '+492041234567'),
(38, 'Xander', 'Rivera', 'ENG', 'GER', '1989-12-07', 24524, '+492051234567'),
(39, 'Yvonne', 'Reed', 'JPN', 'FRA', '1983-06-30', 25625, '+492061234567'),
(40, 'Zach', 'Murphy', 'CHI', 'ENG', '1976-05-18', 26726, '+492071234567'),
(41, 'Amber', 'Cooper', 'ENG', 'SPA', '1992-02-09', 27827, '+492081234567'),
(42, 'Brad', 'Morgan', 'FRE', 'GER', '1984-11-14', 28928, '+492091234567'),
(43, 'Cara', 'Bell', 'GER', 'CHI', '1979-01-05', 29029, '+492101234567'),
(44, 'Dean', 'Russell', 'ITA', 'RUS', '1986-03-08', 30130, '+492111234567'),
(45, 'Ella', 'Griffin', 'ENG', 'FRE', '1981-09-29', 31231, '+492121234567'),
(46, 'Felix', 'Simmons', 'SPA', 'ENG', '1995-07-11', 32332, '+492131234567'),
(47, 'Gina', 'Hughes', 'GER', 'FRA', '1993-10-15', 33433, '+492141234567'),
(48, 'Harry', 'Ortiz', 'JPN', 'CHI', '1988-02-23', 34534, '+492151234567'),
(49, 'Ivy', 'Hayes', 'ENG', 'RUS', '1977-06-06', 35635, '+492161234567'),
(50, 'Jackie', 'Flores', 'CHI', 'SPA', '1985-04-20', 36736, '+492171234567');
"""

connection = create_db_connection("host","user_name", "user_password", "db_name")
execute_query(connection, pop_teacher)



# def read_query(connection, query):
#     cursor = connection.cursor()
#     result = None
#     try:
#         cursor.execute(query)
#         result = cursor.fetchall()
#         return result
#     except Error as err:
#         print(f"Error: '{err}'")

# connection = create_db_connection("host","user_name", "user_password", "db_name")
# select_teacher_query = "SELECT * FROM teacher;"
# teachers = read_query(connection, select_teacher_query)

# for teacher in teachers:
#     print(teacher)

pop_client = """
INSERT INTO client VALUES
(106, 'Tech Innovators Inc.', '56 Techstraße, 10437 Berlin', 'Technology'),
(107, 'Green Energy Solutions', '89 Ecoweg, 10823 Berlin', 'Energy'),
(108, 'HealthFirst Clinic', '101 Gesundstraße, 10785 Berlin', 'Healthcare'),
(109, 'EduFuture Academy', '34 Wissenstraße, 10623 Berlin', 'Education'),
(110, 'FastFoods Ltd.', '78 Snackstraße, 10965 Berlin', 'Food & Beverage'),
(111, 'TravelMore AG', '45 Holidaystraße, 10557 Berlin', 'Travel'),
(112, 'BookWorld GmbH', '67 Bücherweg, 10115 Berlin', 'Retail'),
(113, 'FitLife Gym', '23 Fitnessstraße, 10439 Berlin', 'Fitness'),
(114, 'SecureBank', '99 Safeweg, 10777 Berlin', 'Banking'),
(115, 'WebCreators', '11 Netweg, 10997 Berlin', 'IT Services'),
(116, 'FashionForward', '23 Modeweg, 10785 Berlin', 'Retail'),
(117, 'GigaGames', '88 Playstraße, 10245 Berlin', 'Entertainment'),
(118, 'BuildIt', '66 Bauweg, 10625 Berlin', 'Construction'),
(119, 'CleanEnergy Co.', '44 Sunstraße, 10829 Berlin', 'Energy'),
(120, 'SmartHome Solutions', '12 Komfortstraße, 10559 Berlin', 'Home Improvement'),
(121, 'QuickMed', '90 Healthweg, 10435 Berlin', 'Healthcare'),
(122, 'EliteEducation', '77 Studyweg, 10629 Berlin', 'Education'),
(123, 'GourmetDelights', '55 Foodstraße, 10961 Berlin', 'Food & Beverage'),
(124, 'TravelWonders', '36 Journeyweg, 10553 Berlin', 'Travel'),
(125, 'GlobalBooks', '48 Readstraße, 10117 Berlin', 'Retail'),
(126, 'ActiveFitness', '52 Exerciseweg, 10437 Berlin', 'Fitness'),
(127, 'MoneySafe Bank', '14 Bankstraße, 10779 Berlin', 'Banking'),
(128, 'CodeMasters', '21 Programmweg, 10999 Berlin', 'IT Services'),
(129, 'StyleHub', '19 Fashionweg, 10783 Berlin', 'Retail'),
(130, 'GameOn', '83 Funstraße, 10247 Berlin', 'Entertainment'),
(131, 'SolidBuilders', '95 Constructweg, 10623 Berlin', 'Construction'),
(132, 'RenewableFuture', '29 Greenstraße, 10821 Berlin', 'Energy'),
(133, 'HomeComfort', '34 Cozyweg, 10557 Berlin', 'Home Improvement'),
(134, 'HealthPlus', '22 Wellnessstraße, 10431 Berlin', 'Healthcare'),
(135, 'FutureLearning', '40 Knowledgeweg, 10627 Berlin', 'Education'),
(136, 'FoodFest', '31 Tasteweg, 10959 Berlin', 'Food & Beverage'),
(137, 'AdventureTravel', '47 Exploreweg, 10551 Berlin', 'Travel'),
(138, 'BookHaven', '74 Pageweg, 10119 Berlin', 'Retail'),
(139, 'GymHeroes', '58 Workoutstraße, 10433 Berlin', 'Fitness'),
(140, 'TrustBank', '87 Secureweg, 10781 Berlin', 'Banking'),
(141, 'DevPros', '93 Codeweg, 10995 Berlin', 'IT Services'),
(142, 'TrendSetter', '25 Styleweg, 10787 Berlin', 'Retail'),
(143, 'PlayZone', '11 Gamerstraße, 10249 Berlin', 'Entertainment'),
(144, 'ConstructPro', '63 Buildweg, 10621 Berlin', 'Construction'),
(145, 'CleanTech', '28 Solarstraße, 10825 Berlin', 'Energy'),
(146, 'HomeEase', '77 Relaxweg, 10561 Berlin', 'Home Improvement'),
(147, 'WellnessWay', '30 Healthweg, 10433 Berlin', 'Healthcare'),
(148, 'SmartLearning', '62 Teachstraße, 10629 Berlin', 'Education'),
(149, 'YummyFoods', '39 Cuisineweg, 10963 Berlin', 'Food & Beverage'),
(150, 'Globetrotter Travel', '25 Wanderweg, 10557 Berlin', 'Travel');
"""

pop_participant = """
INSERT INTO participant VALUES
(115, 'Lara', 'Klein', '491625552134', 106),
(116, 'Stefan', 'Schmidt', '491625551234', 107),
(117, 'Nina', 'Müller', '491625553234', 108),
(118, 'Markus', 'Schneider', '491625554234', 109),
(119, 'Tanja', 'Fischer', '491625555234', 110),
(120, 'Oliver', 'Weber', '491625556234', 111),
(121, 'Kerstin', 'Wagner', '491625557234', 112),
(122, 'Sebastian', 'Becker', '491625558234', 113),
(123, 'Andrea', 'Hofmann', '491625559234', 114),
(124, 'Florian', 'Koch', '491625551234', 115),
(125, 'Carla', 'Richter', '491625552345', 116),
(126, 'Dennis', 'Wolf', '491625553456', 117),
(127, 'Hannah', 'Bauer', '491625554567', 118),
(128, 'Fabian', 'Jung', '491625555678', 119),
(129, 'Julia', 'Krüger', '491625556789', 120),
(130, 'Sophie', 'Hartmann', '491625557890', 121),
(131, 'Julian', 'Schulz', '491625558901', 122),
(132, 'Anna', 'Maier', '491625559012', 123),
(133, 'Lukas', 'König', '491625550123', 124),
(134, 'Melanie', 'Krause', '491625551234', 125),
(135, 'Simon', 'Peters', '491625552345', 126),
(136, 'Laura', 'Lang', '491625553456', 127),
(137, 'Patrick', 'Mayer', '491625554567', 128),
(138, 'Kathrin', 'Fuchs', '491625555678', 129),
(139, 'Timo', 'Herrmann', '491625556789', 130),
(140, 'Marie', 'Keller', '491625557890', 131),
(141, 'Jonas', 'Schmidt', '491625558901', 132),
(142, 'Katharina', 'Lehmann', '491625559012', 133),
(143, 'Alexander', 'Fischer', '491625550123', 134),
(144, 'Verena', 'Schwarz', '491625551234', 135),
(145, 'Leon', 'Schubert', '491625552345', 136),
(146, 'Eva', 'Kaufmann', '491625553456', 137),
(147, 'Paul', 'Vogel', '491625554567', 138),
(148, 'Sarah', 'Arnold', '491625555678', 139),
(149, 'David', 'Lorenz', '491625556789', 140),
(150, 'Johanna', 'Dietrich', '491625557890', 141),
(151, 'Philipp', 'Roth', '491625558901', 142),
(152, 'Lisa', 'Zimmermann', '491625559012', 143),
(153, 'Robert', 'Seidel', '491625550123', 144),
(154, 'Sandra', 'Götz', '491625551234', 145),
(155, 'Matthias', 'Berg', '491625552345', 146),
(156, 'Nicole', 'Haas', '491625553456', 147),
(157, 'Andreas', 'Metzger', '491625554567', 148),
(158, 'Susanne', 'Pohl', '491625555678', 149),
(159, 'Daniel', 'Brandt', '491625556789', 150);
"""

pop_course = """
INSERT INTO course VALUES
(21, 'Business English', 'ENG', 'B2', 12, '2020-05-01', TRUE, 7, 106),
(22, 'English for IT', 'ENG', 'B1', 8, '2020-06-15', FALSE, 8, 107),
(23, 'Medical English', 'ENG', 'C1', 10, '2020-07-01', TRUE, 9, 108),
(24, 'English for Academics', 'ENG', 'C2', 15, '2020-08-01', FALSE, 10, 109),
(25, 'English for Hospitality', 'ENG', 'A2', 6, '2020-09-01', TRUE, 11, 110),
(26, 'Advanced Business English', 'ENG', 'C1', 12, '2020-10-01', FALSE, 12, 111),
(27, 'Technical English', 'ENG', 'B2', 8, '2020-11-01', TRUE, 13, 112),
(28, 'Legal English', 'ENG', 'C2', 10, '2020-12-01', FALSE, 14, 113),
(29, 'Conversational English', 'ENG', 'A1', 5, '2021-01-01', TRUE, 15, 114),
(30, 'Scientific English', 'ENG', 'C1', 20, '2021-02-01', FALSE, 16, 115),
(31, 'English for Engineers', 'ENG', 'B2', 12, '2021-03-01', TRUE, 17, 116),
(32, 'English for Marketing', 'ENG', 'C1', 10, '2021-04-01', FALSE, 18, 117),
(33, 'English for Journalism', 'ENG', 'C2', 15, '2021-05-01', TRUE, 19, 118),
(34, 'English for Entrepreneurs', 'ENG', 'B1', 8, '2021-06-01', FALSE, 20, 119),
(35, 'English for Designers', 'ENG', 'B2', 10, '2021-07-01', TRUE, 21, 120),
(36, 'English for Finance', 'ENG', 'C1', 12, '2021-08-01', FALSE, 22, 121),
(37, 'English for Education', 'ENG', 'B1', 8, '2021-09-01', TRUE, 23, 122),
(38, 'English for Healthcare', 'ENG', 'C2', 10, '2021-10-01', FALSE, 24, 123),
(39, 'English for HR', 'ENG', 'B2', 12, '2021-11-01', TRUE, 25, 124),
(40, 'English for Real Estate', 'ENG', 'A2', 6, '2021-12-01', FALSE, 26, 125),
(41, 'English for Tourism', 'ENG', 'B1', 8, '2022-01-01', TRUE, 27, 126),
(42, 'English for Media', 'ENG', 'C1', 10, '2022-02-01', FALSE, 28, 127),
(43, 'English for IT Security', 'ENG', 'C2', 12, '2022-03-01', TRUE, 29, 128),
(44, 'English for Architecture', 'ENG', 'B2', 8, '2022-04-01', FALSE, 30, 129),
(45, 'English for Environment', 'ENG', 'C1', 10, '2022-05-01', TRUE, 31, 130),
(46, 'English for Law Enforcement', 'ENG', 'B2', 12, '2022-06-01', FALSE, 32, 131),
(47, 'English for Logistics', 'ENG', 'A1', 6, '2022-07-01', TRUE, 33, 132),
(48, 'English for Public Speaking', 'ENG', 'C2', 15, '2022-08-01', FALSE, 34, 133),
(49, 'English for Personal Development', 'ENG', 'B1', 8, '2022-09-01', TRUE, 35, 134),
(50, 'English for Project Management', 'ENG', 'C1', 10, '2022-10-01', FALSE, 36, 135);
"""

pop_takescourse = """
INSERT INTO takes_course VALUES
(115, 21),
(116, 22),
(117, 23),
(118, 24),
(119, 25),
(120, 26),
(121, 27),
(122, 28),
(123, 29),
(124, 30),
(125, 31),
(126, 32),
(127, 33),
(128, 34),
(129, 35),
(130, 36),
(131, 37),
(132, 38),
(133, 39),
(134, 40),
(135, 41),
(136, 42),
(137, 43),
(138, 44),
(139, 45),
(140, 46),
(141, 47),
(142, 48),
(143, 49),
(144, 50),
(145, 21),
(146, 22),
(147, 23),
(148, 24),
(149, 25),
(150, 26),
(151, 27),
(152, 28),
(153, 29),
(154, 30),
(155, 31),
(156, 32),
(157, 33),
(158, 34),
(159, 35);
"""

connection = create_db_connection("host","user_name", "user_password", "db_name")
execute_query(connection, pop_client)
execute_query(connection, pop_participant)
execute_query(connection, pop_course)
execute_query(connection, pop_takescourse)


####---------------------------------------------------------------------------------------#####

### Reading Data with Python

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")
        
# q1 = """
# select *
# from teacher;
# """


# connection = create_db_connection("host","user_name", "user_password", "db_name")
# results = read_query(connection, q1)

# for result in results:
#     print(result)


q5 = """
SELECT course.course_id, course.course_name, course.language, client.client_name, client.address
FROM course
JOIN client
ON course.client = client.client_id
WHERE course.in_school = FALSE;
"""

connection = create_db_connection("host","user_name", "user_password", "db_name")
results = read_query(connection, q5)

for result in results:
  print(result)
  

# q6 = """
# SELECT participant.participant_id, participant.phone_no, participant.last_name
# FROM participant;
# """

# connection = create_db_connection("host","user_name", "user_password", "db_name")
# results = read_query(connection, q6)

# for read in results:
#   print(results)