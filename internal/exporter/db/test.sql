/*Load database*/

/*Set environment variables of files location*/
\set pagesfilepath :datafilepath pages.csv
\set revisionsfilepath :datafilepath revisions.csv
\set socialjumpsfilepath :datafilepath socialjumps.csv

\i base.sql;
\i indices/default.sql;
/*\i indices/alternate.sql;*/


/*Test queries on database*/

/*Disable pager for testing queries*/
\pset pager off

\i types.sql;
\i query-toptenbyyear.sql;
\i query-pages.sql;