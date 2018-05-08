/*
psql -v datafilepath='abs/path/2/CSV' -c '\i test.sql';
*/

\pset pager off;

\set pagesfilepath :datafilepath 'pages.csv';
\set revisionsfilepath :datafilepath 'revisions.csv';
\set socialjumpsfilepath :datafilepath 'socialjumps.csv';

\i base.sql;
\i indices.sql;
\i types.sql;
\i query-toptenbyyear.sql;
\i query-pages.sql;
