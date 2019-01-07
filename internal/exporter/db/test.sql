/*Load database*/
\i base.sql;
\i indices/default.sql;
/*\i indices/alternate.sql;*/

/*Disable pager for testing queries*/
\pset pager off

/*Test queries on database*/
\i types.sql;
\i query-toptenbyyear.sql;
\i query-pages.sql;