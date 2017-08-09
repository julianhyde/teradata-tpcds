/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.teradata.tpcds.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.teradata.tpcds.distribution.Distribution;
import com.teradata.tpcds.distribution.DistributionUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/** Utilities for {@link Substitution}. */
class Substitutions
{
    private Substitutions() {}

    /** Creates a substitution that returns the same string every time. */
    static Substitution fixed(final String s)
    {
        return generator -> s;
    }

    /** Creates a substitution that returns the number of rows in a relation. */
    static Substitution rowCount(final String relation)
    {
        return generator -> generator.rowCount(relation);
    }

    /** Creates a substitution that generates a list. */
    public static Substitution list(int count, Substitution substitution)
    {
        return new ListSubstitution(substitution, count);
    }

    /** Creates a substitution that applies a function to another substitution. */
    static Substitution transform(final Substitution substitution,
                                  final Function<String, String> function)
    {
        return generator -> {
            final String s = substitution.generate(generator);
            return function.apply(s);
        };
    }

    /** Creates a substitution that generates uniform values over an integer
     * range. The start and end points of the range are defined by
     * substitutions. */
    private static Substitution uniform(Substitution start, Substitution end)
    {
        return generator -> {
            final String startValue = start.generate(generator);
            final int startInt = Integer.parseInt(startValue);
            final String endValue = end.generate(generator);
            final int endInt = Integer.parseInt(endValue);
            int range = endInt - startInt + 1;
            return Integer.toString(startInt + generator.random.nextInt(range));
        };
    }

    private static String remove(String s, String start, String end)
    {
        assert s.startsWith(start) : s;
        assert s.endsWith(end) : s;
        return s.substring(start.length(), s.length() - end.length());
    }

    private static List<String> parseArgs(String s, String start, String end)
    {
        s = remove(s, start, end);
        final char[] chars = s.toCharArray();
        int parenCount = 0;
        boolean inQuote = false;
        int x = 0;
        final List<String> list = new ArrayList<>();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            switch (c) {
            case '(':
            case '{':
                ++parenCount;
                break;
            case ')':
            case '}':
                --parenCount;
                break;
            case '"':
                inQuote = !inQuote;
                break;
            case ',':
                if (parenCount == 0 && !inQuote) {
                    list.add(s.substring(x, i));
                    while (i + 1 < chars.length && chars[i + 1] == ' ') {
                        ++i;
                    }
                    x = i + 1;
                    break;
                }
            }
        }
        if (chars.length > x) {
            list.add(s.substring(x));
        }
        return list;
    }

    static Substitution parse(String s)
    {
        final String original = s;
        if (s.startsWith("text(")) {
            List<String> args = parseArgs(s, "text(", ")");
            if (args.size() == 1) {
                return fixed(args.get(0));
            }
            final ImmutableList.Builder<Pair> builder =
                ImmutableList.builder();
            for (String arg : args) {
                if (arg.startsWith("{")) {
                    List<String> subArgs = parseArgs(arg, "{", "}");
                    assert subArgs.size() == 2;
                    final String text = subArgs.get(0);
                    final int weight = Integer.parseInt(subArgs.get(1));
                    builder.add(Pair.of(text.substring(1, text.length() - 1), weight));
                }
            }
            return text(builder.build());
        }
        if (s.startsWith("\"")
                && s.endsWith("\"")) {
            return fixed(s.substring(1, s.length() - 1));
        }
        if (s.startsWith("ulist(")) {
            // Example:
            //  ulist(random(10000,99999,uniform),400)
            List<String> args = parseArgs(s, "ulist(", ")");
            int count = Integer.parseInt(args.get(1));
            final Substitution substitution = parse(args.get(0));
            return list(count, substitution);
        }
        if (s.startsWith("date(")) {
            // Example:
            //  date([YEAR]+"-08-01",[YEAR]+"-08-30",sales)
            List<String> args = parseArgs(s, "date(", ")");
            final String min = args.get(0);
            final String max = args.get(1);
            final DateDistribution distribution =
                    DateDistribution.valueOf(args.get(2).toUpperCase());
            final Substitution minSub = parse(min);
            final Substitution maxSub = parse(max);
            return date(minSub, maxSub, distribution);
        }
        if (s.startsWith("rowcount(")) {
            final int divide;
            if (s.matches(".*/[0-9]+$")) {
                // Example:
                //  rowcount("store_sales")/5
                int slash = s.lastIndexOf('/');
                divide = Integer.parseInt(s.substring(slash + 1));
                s = s.substring(0, slash);
            }
            else {
                divide = 1;
            }
            // Example:
            //  rowcount("active_counties", "store")
            List<String> args = parseArgs(s, "rowcount(", ")");
            final String relation = args.get(args.size() - 1);
            final Substitution substitution = fixed("100");
            if (divide > 1) {
                return divide(divide, substitution);
            }
            return substitution;
        }
        if (s.startsWith("distmember(")) {
            // Example:
            //  distmember(fips_county, [COUNTY], 3)
            //  distmember(i_manager_id, [MGR_IDX], 2)
            List<String> args = parseArgs(s, "distmember(", ")");
            final Distribution distribution =
                    DistributionUtils.distribution(args.get(0)); // e.g. "fips_county"
            final String ref = args.get(1);
            final Substitution refSubstitution = parse(ref);
            final int field = Integer.parseInt(args.get(2));
            return distributionMember(refSubstitution, distribution, field);
        }
        if (s.startsWith("DIST(")) {
            s = "dist" + s.substring("DIST".length());
        }
        if (s.startsWith("dist(")) {
            // Example:
            //  dist(gender, 1, 1)
            List<String> args = parseArgs(s, "dist(", ")");
            String distributionName = args.get(0); // e.g. "fips_county"
            if (distributionName.equals("distmember(categories,[CINDX],2)")) {
                // One of the queries has
                // "dist(distmember(categories,[CINDX],2),1,1)"
                // I honestly have no idea what that means.
                distributionName = "categories";
            }
            final Distribution distribution =
                    DistributionUtils.distribution(distributionName);
            final int field = Integer.parseInt(args.get(1));
            final String weightName = args.get(2);
            int weight = distribution.getWeightNames().indexOf(weightName);
            if (weight < 0) {
                try {
                    weight = Integer.parseInt(weightName);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unknown weight "
                            + weightName);
                }
            }
            return distribution(distribution, field, weight);
        }
        if (s.startsWith("random(")) {
            List<String> parts = parseArgs(s, "random(", ")");
            assert parts.size() == 3 : s;
            assert parts.get(2).equals("uniform") : s;
            final Substitution start = parse(parts.get(0));
            final Substitution end = parse(parts.get(1));
            return uniform(start, end);
        }
        if (s.startsWith("[")
            && s.contains("]")) {
            int close = s.indexOf("]");
            final Substitution sub = ref(s.substring(1, close));
            s = s.substring(close + 1);
            if (s.equals("")) {
                return sub;
            }
            if (s.startsWith("+")) {
                final Substitution next = parse(s.substring(1));
                return concatenate(sub, next);
            }
            throw new IllegalArgumentException("unknown pattern: " + s);
        }
        try {
            int i = Integer.valueOf(s);
            return fixed(s);
        }
        catch (NumberFormatException e) {
            throw new AssertionError("unknown substitution: " + s + " (original="
                    + original + ")");
        }
    }

    private static Substitution distribution(Distribution distribution,
            int field, int weight)
    {
        return generator ->
                toString(distribution.random(field, weight, generator.random));
    }

    private static Substitution concatenate(Substitution s0, Substitution s1)
    {
        return generator -> s0.generate(generator) + s1.generate(generator);
    }

    private static Substitution date(Substitution min, Substitution max,
            DateDistribution distribution)
    {
        return generator -> {
            final Date minDate = toDate(min.generate(generator));
            final Date maxDate = toDate(max.generate(generator));
            final long minTime = minDate.getTime();
            final long maxTime = maxDate.getTime();
            final long time = minTime
                + generator.random.nextLong() % (maxTime - minTime);
            return toString(new Date(time));
        };
    }

    private static Date toDate(String s)
    {
        try {
            return new SimpleDateFormat("yyyy-MM-dd").parse(s);
        }
        catch (ParseException e) {
            throw new RuntimeException("bad date: " + s);
        }
    }

    private static String toString(Date s)
    {
        return new SimpleDateFormat("yyyy-MM-dd").format(s);
    }

    private static Substitution divide(int divide, Substitution substitution)
    {
        return generator -> {
            final String s = substitution.generate(generator);
            final int v = Integer.parseInt(s);
            return String.valueOf(v / divide);
        };
    }

    private static Substitution distributionMember(
            Substitution substitution, Distribution distribution, int field)
    {
        return generator -> {
            final int index = Integer.parseInt(substitution.generate(generator));
            return toString(distribution.cell(field, index));
        };
    }

    private static String toString(Object o)
    {
        return o instanceof Date
                ? toString((Date) o)
                : o.toString();
    }

    private static Substitution ref(String ref)
    {
        return generator -> {
            final Object value = generator.substitutionValues.get(ref);
            if (value != null) {
                return toString(value);
            }
            final Substitution substitution = generator.substitutions.get(ref);
            if (substitution == null) {
                throw new IllegalArgumentException("Unknown substitution: " + ref);
            }
            final String value2 = substitution.generate(generator);
            generator.substitutionValues.put(ref, value2);
            return value2;
        };
    }

    public static Substitution item(String ref, int i)
    {
        return generator -> {
            @SuppressWarnings("unchecked") final List<String> list =
                    (List) generator.substitutionValues.get(ref);
            if (list != null) {
                return toString(list.get(i));
            }
            final Substitution substitution = generator.substitutions.get(ref);
            if (substitution == null) {
                throw new IllegalArgumentException("Unknown substitution: " + ref);
            }
            final List<String> value2 =
                    ((ListSubstitution) substitution).generateList(generator);
            generator.substitutionValues.put(ref, value2);
            return toString(value2.get(i));
        };
    }

    private static Substitution text(final ImmutableList<Pair> map)
    {
        return generator -> {
            int n = 0;
            for (Pair pair : map) {
                n += pair.i;
            }
            final int r = generator.random.nextInt(n);
            int x = 0;
            for (Pair pair : map) {
                x += pair.i;
                if (x >= r) {
                    return pair.s;
                }
            }
            throw new AssertionError();
        };
    }

    /** Evaluates a substitution that is constant (references no other
     * substitutions). */
    static String constant(Substitution substitution)
    {
        // dummy generator
        final Generator g =
            new Generator(new Random(0), ImmutableMap.of());
        return substitution.generate(g);
    }

    /** String-int pair. */
    static class Pair
    {
        final String s;
        final int i;

        private Pair(String s, int i)
        {
            this.s = s;
            this.i = i;
        }

        static Pair of(String s, int i)
        {
            return new Pair(s, i);
        }
    }

    static class ListSubstitution implements Substitution
    {
        final Substitution substitution;
        final int count;

        ListSubstitution(Substitution substitution, int count)
        {
            this.substitution = substitution;
            this.count = count;
        }

        public String generate(Generator generator)
        {
            return generateList(generator).toString();
        }

        private List<String> generateList(Generator generator)
        {
            final Set<String> strings = new HashSet<>();
            final int limit = count * 2 + 10000;
            int attempt = 0;
            final List<String> list = new ArrayList<>();
            while (list.size() < count) {
                final String s = substitution.generate(generator);
                if (strings.add(s) || attempt++ > limit) {
                    // After a reasonable number of attempts to produce unique
                    // entries, settle for non-unique ones.
                    list.add(s);
                }
            }
            return ImmutableList.copyOf(list);
        }
    }

    enum DateDistribution
    {
        SALES,
        RETURNS,
        EXPONENTIAL,
        UNIFORM
    }
}
