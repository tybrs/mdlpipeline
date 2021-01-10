from datetime import date
from .query_tools import *

TERM = 'Winter'
YEAR = '2021'
TERM_YEAR = TERM + YEAR

@query_to_df
def get_courses(db_h, term=TERM, year=YEAR):
    return """SELECT distinct sxn.EVENT_ID,sxn.EVENT_LONG_NAME 
              FROM SECTIONS sxn 
              WHERE sxn.ACADEMIC_TERM = '{term}' 
              AND sxn.ACADEMIC_YEAR = '{year}' 
              ORDER BY sxn.EVENT_ID""".format(term=term, year=year)

@query_to_df
def get_roster(db_h, *args, term=TERM, year=YEAR, print=True):
    query = """SELECT DISTINCT
                dbo.SECTIONPER.EVENT_ID, dbo.SECTIONPER.SECTION, dbo.TRANSCRIPTDETAIL.EVENT_LONG_NAME,
                dbo.PEOPLE.FIRST_NAME AS StudentFirstName, dbo.PEOPLE.LAST_NAME AS StudentLastName,
                EA.Email, REPLACE(PEOPLE.PEOPLE_CODE_ID,'P','') as idnumber

            FROM dbo.TRANSCRIPTDETAIL
                INNER JOIN dbo.SECTIONPER
                    ON dbo.TRANSCRIPTDETAIL.ACADEMIC_YEAR = dbo.SECTIONPER.ACADEMIC_YEAR
                        AND dbo.TRANSCRIPTDETAIL.ACADEMIC_TERM = dbo.SECTIONPER.ACADEMIC_TERM
                        AND dbo.TRANSCRIPTDETAIL.EVENT_ID = dbo.SECTIONPER.EVENT_ID
                LEFT OUTER JOIN dbo.PEOPLE
                    ON dbo.TRANSCRIPTDETAIL.PEOPLE_CODE_ID = dbo.PEOPLE.PEOPLE_CODE_ID
                LEFT OUTER JOIN  EmailAddress AS EA
                    ON PEOPLE.PEOPLE_CODE_ID = EA.PeopleOrgCodeId
                    AND EA.EmailType = 'UWSSTU'

            WHERE (dbo.SECTIONPER.ACADEMIC_YEAR = N'{year}')
                AND (dbo.SECTIONPER.ACADEMIC_TERM = N'{term}')""".format(term=term, year=year)
    if len(args) > 1:
        query += "\n AND dbo.TRANSCRIPTDETAIL.EVENT_ID in {course}".format(course=args)
    else:
        query += "\n AND dbo.TRANSCRIPTDETAIL.EVENT_ID = '{course}'".format(course=args[0])
    return query

@query_to_df
def get_all_rosters(db_h, *args, term=TERM, year=YEAR, print=True):
    courses_str = str(args).replace(',)', ')')
    query = """SELECT DISTINCT REPLACE(PEOPLE.PEOPLE_CODE_ID,'P','') as idnumber,
                SECTIONS.EVENT_ID as course, dbo.TRANSCRIPTDETAIL.SECTION as section,
                CASE
                    WHEN dbo.TRANSCRIPTDETAIL.FINAL_GRADE=N'AU' THEN 'auditingstudent'
                    ELSE 'student'
                END AS role
                FROM dbo.TRANSCRIPTDETAIL
                INNER JOIN dbo.SECTIONS
                    ON dbo.TRANSCRIPTDETAIL.ACADEMIC_YEAR = dbo.SECTIONS.ACADEMIC_YEAR
                        AND dbo.TRANSCRIPTDETAIL.ACADEMIC_TERM = dbo.SECTIONS.ACADEMIC_TERM
                        AND dbo.TRANSCRIPTDETAIL.EVENT_ID = dbo.SECTIONS.EVENT_ID
                    LEFT OUTER JOIN dbo.PEOPLE
                        ON dbo.TRANSCRIPTDETAIL.PEOPLE_CODE_ID = dbo.PEOPLE.PEOPLE_CODE_ID
                    LEFT OUTER JOIN dbo.ACADEMIC
                        ON dbo.ACADEMIC.PEOPLE_CODE_ID = dbo.TRANSCRIPTDETAIL.PEOPLE_CODE_ID AND
                        dbo.ACADEMIC.academic_year = dbo.TRANSCRIPTDETAIL.academic_year AND
                        dbo.ACADEMIC.academic_term = dbo.TRANSCRIPTDETAIL.academic_term AND
                        dbo.ACADEMIC.academic_session = dbo.TRANSCRIPTDETAIL.academic_session
                WHERE dbo.SECTIONS.ACADEMIC_YEAR = N'{year}'
                    AND dbo.SECTIONS.ACADEMIC_TERM = N'{term}'
                    AND dbo.TRANSCRIPTDETAIL.ADD_DROP_WAIT = N'A'
                    AND (dbo.TRANSCRIPTDETAIL.FINAL_GRADE != N'W' AND dbo.TRANSCRIPTDETAIL.FINAL_GRADE != N'WA')
                    AND (dbo.ACADEMIC.ENROLL_SEPARATION != 'LOA' AND dbo.ACADEMIC.ENROLL_SEPARATION != 'WITH'
                        AND dbo.ACADEMIC.ENROLL_SEPARATION != 'AWIT')
                    AND dbo.TRANSCRIPTDETAIL.EVENT_ID in {course}""".format(term=term, year=year, course=courses_str)
    return query


@query_to_df
def get_students_by_program(db_h, term=TERM, year=YEAR, print=True):
    query = """SELECT DISTINCT REPLACE(aca.PEOPLE_CODE_ID,'P','') as idnumber, aca.CURRICULUM as program
            FROM ACADEMIC AS aca
            JOIN PEOPLE AS ppl on ppl.PEOPLE_CODE_ID = aca.PEOPLE_CODE_ID
            WHERE aca.CURRICULUM in ('CHIRO', 'NUTRIT', 'CLMH')
            and aca.ACADEMIC_YEAR = N'{year}' and aca.ACADEMIC_TERM = N'{term}'""".format(term=term, year=year)
    return query