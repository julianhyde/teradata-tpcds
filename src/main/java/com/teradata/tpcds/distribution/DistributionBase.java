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

package com.teradata.tpcds.distribution;

import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.List;
import java.util.Random;

public class DistributionBase<E> implements Distribution
{
    protected final ImmutableList<ImmutableList<E>> valuesLists;
    protected final ImmutableList<ImmutableList<Integer>> weightsLists;
    protected ImmutableList<String> weightNames;

    protected DistributionBase(ImmutableList<ImmutableList<E>> valuesLists,
            ImmutableList<ImmutableList<Integer>> weightsLists,
            List<String> weightNames)
    {
        this.valuesLists = valuesLists;
        this.weightsLists = weightsLists;
        this.weightNames = ImmutableList.copyOf(weightNames);
    }

    protected DistributionBase(ImmutableList<ImmutableList<E>> valuesLists,
            ImmutableList<ImmutableList<Integer>> weightsLists)
    {
        this(valuesLists, weightsLists, generateWeightNames(weightsLists.size()));
    }

    /** Generates a list of numeric weight names,
     * e.g. generateWeightNames(3) returns ["1", "2", "3"]. */
    protected static List<String> generateWeightNames(int count)
    {
        return new AbstractList<String>() {
            public String get(int index)
            {
                return Integer.toString(index + 1);
            }

            public int size()
            {
                return count;
            }
        };
    }

    public int getSize()
    {
        return valuesLists.get(0).size();
    }

    protected List<E> list(int field)
    {
        return valuesLists.get(field);
    }

    public Object cell(int field, int row)
    {
        return list(field).get(row);
    }

    public Object random(int field, int weight, Random random)
    {
        List list = list(field);
        return list.get(random.nextInt(list.size()));
    }

    public List<String> getWeightNames()
    {
        return weightNames;
    }
}
