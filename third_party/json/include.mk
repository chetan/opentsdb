# Copyright (C) 2012  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

JSON_VERSION := chargebee-1.0
JSON := third_party/json/org.json-$(JSON_VERSION).jar
JSON_BASE_URL := http://search.maven.org/remotecontent?filepath=org/json/org.json/$(JSON_VERSION)

$(JSON): $(JSON).md5
	set dummy "$(JSON_BASE_URL)" "$(JSON)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(JSON)
