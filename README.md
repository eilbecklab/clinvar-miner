## Getting started

_Note: If developing on Windows, use of the
[Windows Subsystem For Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
(WSL) is strongly recommended._

1. Install Make, cURL, Python 3, and Pip 3 from your system package manager.

2. Run `pip3 install -r requirements.txt` to install required Python packages.

3. Run `make countries` to update information about ClinVar submitters and their
   countries.

4. Check `submitter_info.tsv` to make sure that that the country information is
   complete and correct (`git diff` is an easy way to look). Make any needed
   changes.

5. Run `make` to build the ClinVar Miner database. This process takes about 1
   hour.

6. For **development**, run `./start-dev.sh` and open http://localhost:5000/ in
   your web browser. You can change the port number by passing `-p <port>`.

   For **production**, point your web server to `clinvar-miner.wsgi`. For
   example, if you are using Apache, the configuration would look like:

   ```
   Timeout 300
   WSGIDaemonProcess wsgi
   WSGIProcessGroup wsgi
   WSGIScriptAlias /clinvar-miner /var/www/clinvar-miner/clinvar-miner.wsgi
   WSGIApplicationGroup %{GLOBAL}
   LimitRequestLine 1000000
   LimitRequestFieldSize 1000000
   ```

7. To update ClinVar Miner after each month's ClinVar release, repeat steps 3,
   4, and 5, and restart the web server with e.g.
   `sudo systemctl restart apache2`.

## License
This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see <http://www.gnu.org/licenses/>.
