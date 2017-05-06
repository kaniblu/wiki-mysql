# Wikipedia Mysql Database Storing Script

This script creates a clean mysql database of wikipedia articles fresh from
the wikidump repo. It can be run on both Python 2 and 3.

## Getting started

Mysql Database must be set up first. create a database and a user with relevant
privileges on the database.

Then simply run `dbfy.py` with relevant configurations. Details on each option
can be seen by running `python dbfy.py --help`.

An example of a script command is as follows.

    python dbfy.py --host wiki --user wiki --passwd hunter123 --db wiki

This will initialize the database with the correct tables and then
download wikipedia pages dump from the latest official repo
(https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2).
Finally it will then iterate through each page and filter according to logics
in `filters.py` and store it in the database.

### White and blacklisting unicode chars

Character filtering is supported at the most basic level from the command line
arguments. `valid_unichrs` options accepts ranges of unicodes in `0x0000,0xFFFF`
form. Multiple `valid_unichrs` are accepted as well. For example,

    python dbfy.py --host wiki --user wiki --passwd hunter123 --db wiki
        --valid_unichrs 0x0020,0x007e --valid_unichrs 0x1004,0x1084

command line options above will ensure that only unicode characters ranging
from (0x0020, 0x007e) and (0x1004, 0x1084) will be pass through the filter.

Unicodes can be blacklisted using a similar option.

    python dbfy.py ... --invalid_unichrs 0x0000,0x001f

Anything ranges listed in `invalid_unichrs` will be filtered out after characters
have been filtered in by `valid_unichrs`.

## Database Schema

The database built by the script is simple: there are two tables `bodies` and
`articles`. As multiple articles could share the same article body due to
redirection mechanism, this schema design ensures that duplicate bodies are
not stored multiple times. Each `article` in articles will reference one of the
article `bodies` by their ids. `bodies` table is simply a collection of clean
article text bodies identified by their `id`s.