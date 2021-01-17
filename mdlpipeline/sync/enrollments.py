from mdlpipeline.utils.mdltools.mdl_connect import *
from mdlpipeline.utils.sqltools.queries import *
from mdlpipeline.utils.mdltools.conduit import put_conduit_file
from mdlpipeline.utils.sqltools.pc_connect import PcConnect

import pandas as pd
import json
from functools import reduce
from datetime import datetime
from urllib.error import HTTPError
from aiohttp import ClientSession
import asyncio

TERM = 'Winter'
YEAR = '2021'
TERM_YEAR = TERM + YEAR

class SyncEnrollments():
    """SyncEnrollments contains a end-to-end ETL pipeline for syncronizing
    enrollments from PowerCampus (SIS) and Moodle (LMS). First, it pulling
    enrollment data from both a PowerCampus production database and Moodle
    Web Services API, computing the difference in rosters, translating
    results intoConduit CSV file format, and pushing a file to Moodles'
    Conduit middleware system via SFTP.

    Args:
        mapping (dict) : Dictionary of JSON mapping file with Moodle
        shortname as key and dictionary containing "courses" (list of str),
        "section" (str), "id" (int) as key-values.

    Attributes:
        mapping (dict): Dictionary of JSON mapping file 
        courseids (list): List of all Moodle courseids to syncronize
        sync_audits (bool): True to sync auditing students, False otherwise.
        wc_rosters (dict): Dictionary of course name and section as key and
        dictionary with "students" and "auditingstudents" keys containing
        sets of student idnumbers as values.
        pc_rosters (dict):Dictionary with course and section name as key
        and dictionary containing student roles as keys and
        sets of student idnumbers as values.
        conduit_dfs (dict): Dictionary of Pandas DataFrames to be pushed
    """
    def __init__(self, mapping):
        self.mapping = mapping
        self.courseids = [self.mapping[course]["id"] for course in self.mapping]
        self.sync_audits = True
        self.wc_pull_errors = []
        self.wc_rosters = {}
        self.pc_rosters = {}
        self.conduit_dfs = {}

    @classmethod
    async def pull(cls, mapping):
        """Primary class factory for creating a SyncEnrollments
        instance and running extract methods for data pull.

        Args:
            mapping (dict) : Dictionary of JSON mapping file with Moodle
            shortname as key and dictionary containing "courses" (list of str),
            "section" (str), "id" (int) as key-values.

        Returns:
            SyncEnrollments: instance of class with  values for `pc_roster`,
            `wc_roster`, and `conduit_dfs` attributes.
        """
        self = cls(mapping)
        # pulls from Moodle WebServices API via asynchonous POST
        self.wc_rosters = await self.get_wc_rosters()
        # pulls from PowerCampus SQL database with pymssql handler
        self.pc_rosters = self.get_pc_rosters()
        self.conduit_dfs['enrollments'] = self.get_conduit_enrollments('student')

        if self.sync_audits:
            self.conduit_dfs['enrollments'] = pd.concat([
                self.conduit_dfs['enrollments'],
                self.get_conduit_enrollments('auditingstudent')])
        return self

    async def get_wc_rosters(self):
        """Wrapper to get enrollment data from Moodle
        WebServices API asynchronously.

        Returns:
            dict: Dictionary of courseid and as key and dictionary
        with "students" and "auditingstudents" keys containing sets
        of student idnumbers as values.

        Example:
            {4423: {"student": {0023} "auditingstudent": {}}}
        """
        wcr = WebCampusRosters(self.courseids)
        rosters = await wcr.get_rosters()
        self.wc_pull_errors = wcr.errors
        return rosters

    def get_courses(self):
        """Get list of courses and list of sectioned courses

        Returns:
            tuple: tuple of lists first containing all course names
            without sections and second containing all course names
            with sections.
        """
        courses = [course for key in self.mapping
                    for course in self.mapping[key]["courses"]
                    if not self.mapping[key].get('section', False)]
        sections = [course for key in self.mapping
                    for course in self.mapping[key]["courses"]
                    if self.mapping[key].get('section')]
        return courses, sections

    def transform_rosters(self, rosters):
        """Transform Pandas DataFrame into dictionary with course
        and section name as key and dictionary containing studnet
        roles as keys and sets of student idnumbers as values.

        Args:
            rosters (DataFrame): Dataframe containg "course", "section",
            "role", and "idnumber" as columns.

        Returns:
            dict: dictionary with course and section name as key
            and dictionary containing student roles as keys and
            sets of student idnumbers as values.

        Example:
            >>> self.transform_rosters(rosters)
            {("Kniting", "01"): {"student": {0023} "auditingstudent": {}}}
        """
        rosters_grouped = rosters.groupby(['course', 'section', 'role'])
        rosters_dict = {}
        for (course, section, role), df in rosters_grouped:
            if (course, section) in rosters_dict:
                rosters_dict[(course, section)].update({role: set(df.idnumber)})
            else:
                rosters_dict[(course, section)] = {role: set(df.idnumber)}
        return rosters_dict

    def get_pc_rosters(self):
        """Pulls all PowerCampus enrollment data for term
        via SQL database with `pymssql` handler.

        Returns:
            dict: Dictionary of course name and section as key and
        dictionary with "students" and "auditingstudents" keys containing
        sets of student idnumbers as values.

        Example:
            >>> print(se.get_pc_rosters())
            {("Kniting", "01"): {"student": {0023} "auditingstudent": {}}}
        """
        courses, sections = self.get_courses()
        pc = PcConnect('prod')
        rosters = get_all_rosters(pc, *courses, *sections, term=TERM, year=YEAR, print=False)
        # remove sections for non-sectioned courses
        rosters.loc[rosters.course.apply(lambda x: x in courses), ['section']] = ''
        return self.transform_rosters(rosters)

    def get_conduit_enrollments(self, role):
        """Calculate roster discrepencies and
        create Conduit enrollment table as Pandas DataFrame

        Args:
            role (str): Representing the Moodle role of students to enroll

        Returns:
            pandas.DataFrame: Containing add/drop records to submit to conduit
        """
        records = []

        # Skip courses that have revieved error durring pull
        courses = [course for course in self.mapping
                   if self.mapping[course]["id"] not in self.wc_pull_errors]
        for course in courses:
            pc_roster = self.__combine_pc_course_rosters(course, role)
            wc_roster = self.wc_rosters[self.mapping[course]["id"]][role]

            missing_enroll = pc_roster - wc_roster
            extra_enroll  = wc_roster - pc_roster

            records += [{"action": "add", "shortname": course,
                         "idnumber": idnumber, "role": role}
                        for idnumber in missing_enroll]

            records += [{"action": "drop", "shortname": course,
                         "idnumber": idnumber, "role": role}
                         for idnumber in  extra_enroll]
        return pd.DataFrame(records)

    def log_conduit(self):
        """Log Conduit enrollment as CSV.
        """
        for df_key, df in self.conduit_dfs.items():
            if not df.empty:
                path = f'log/{df_key}-{datetime.now().strftime("%Y-%m-%d %H.%M.%S")}.csv'
                df.to_csv(path, index=False)

    def push_conduit(self):
        """Push Conduit CSV via SFTP
        """
        for df_key, df in self.conduit_dfs.items():
            if not df.empty:
                put_conduit_file(df.to_csv(index=False), df_key + '.csv')
                print(f'{df_key} pushed at {str(datetime.now())}')

    def __combine_pc_course_rosters(self, course, role):
        """Helper function to combine the roster of all courses that are
        cross-listed in `self.mapping`.

        Args:
            course (str): course name
            role (str): role of students to combine rosters for

        Returns:
           set: containg list of all students for all cross-listed courses
        """
        course_section = self.mapping[course].get('section', '')
        crosslist_courses = self.mapping[course]["courses"]
        crosslist_rosters = [self.pc_rosters.get((crosslist, course_section), {}).get(role, set())
                            for crosslist in crosslist_courses]
        return set(reduce(lambda x,y: x | y, crosslist_rosters))

class SyncNonAcademicEnrollments(SyncEnrollments):
    """Creates a ETL pipeline for syncronizing enrollments for
    courses that all students in a particular program or college
    must be enrolled.

    Args:
        mapping (dict) : Dictionary of JSON mapping file with Moodle
        shortname as key and dictionary containing "program" (str),
        and "id" (int) as keys.

    Attributes:
        mapping (dict): Dictionary of JSON mapping file 
        courseids (list): List of all Moodle courseids to syncronize
        sync_audits (bool): True to sync auditing students, False otherwise.
        wc_rosters (dict): Dictionary of course name and section as key and
        dictionary with "students" and "auditingstudents" keys containing
        sets of student idnumbers as values.
        pc_rosters (dict):Dictionary with course and section name as key
        and dictionary containing student roles as keys and
        sets of student idnumbers as values.
        conduit_dfs (dict): Dictionary of Pandas DataFrames to be pushed.
    """
    def __init__(self, mapping):
            super().__init__(mapping)
            self.sync_audits = False

    def get_pc_rosters(self):
        """Pulls all PowerCampus enrollment data for term
        via SQL database with `pymssql` handler.
        """
        pc = PcConnect('prod')
        program_rosters = get_students_by_program(pc)
        return {program: set(df.idnumber) for program, df in program_rosters.groupby('program')}

    def get_conduit_enrollments(self, role):
        """Calculate roster discrepencies and
        create Conduit enrollment table as Pandas DataFrame

        Args:
            role (str): Representing the Moodle role of students to enroll

        Returns:
            pandas.DataFrame: Containing add/drop records to submit to conduit
        """
        records = []
        for course in self.mapping:
            pc_roster = self.pc_rosters[self.mapping[course]["program"]]
            wc_roster = self.wc_rosters[self.mapping[course]["id"]][role]
            missing_enroll = pc_roster - wc_roster           
            records += [{"action": "add", "shortname": course,
                         "idnumber": idnumber, "role": "student"}
                        for idnumber in missing_enroll]
        return pd.DataFrame(records)
