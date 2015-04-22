# Cassandra-Mesos Docs and Website

## Run it locally

Ensure you have installed everything listed in the dependencies section before
following the instructions.

### Dependencies

* [Bundler](http://bundler.io/)
* [Node.js](http://nodejs.org/) (for compiling assets)
* Python
* Ruby
* [RubyGems](https://rubygems.org/)

### Instructions

1. Install packages needed to generate the site

    * On Linux:

            $ apt-get install ruby-dev make autoconf nodejs nodejs-legacy python-dev
    * On Mac OS X:
    
            $ brew install node

2. Clone the Cassandra-Mesos repository

3. Change into the "docs" directory where docs live

        $ cd docs

4. Install Bundler

        $ gem install bundler

5. Install the bundle's dependencies

        $ bundle install

6. Start the web server

        $ bundle exec jekyll serve --watch

7. Visit the site at
   [http://localhost:4000/cassandra-mesos/](http://localhost:4000/cassandra-mesos/)

## Deploying the site

1. Clone a separate copy of the Cassandra-Mesos repo as a sibling of your normal
   Cassandra-Mesos project directory and name it "cassandra-mesos-gh-pages".

        $ git clone git@github.com:mesosphere/cassandra-mesos.git cassandra-mesos-gh-pages

2. Check out the "gh-pages" branch.

        $ cd /path/to/cassandra-mesos-gh-pages
        $ git checkout gh-pages

3. Copy the contents of the "docs" directory in master to the root of your
   cassandra-mesos-gh-pages directory.

        $ cd /path/to/cassandra-mesos
        $ cp -r docs/** ../cassandra-mesos-gh-pages

4. Change to the cassandra-mesos-gh-pages directory, commit, and push the changes

        $ cd /path/to/cassandra-mesos-gh-pages
        $ git commit . -m "Syncing docs with master branch"
        $ git push
