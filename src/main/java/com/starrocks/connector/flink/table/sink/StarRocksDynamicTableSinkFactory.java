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

package com.starrocks.connector.flink.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * 自定义sink工厂类入口，用于SPI发现
 */
public class StarRocksDynamicTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(StarRocksSinkOptions.SINK_PROPERTIES_PREFIX);

        ReadableConfig options = helper.getOptions();
        // validate some special properties
        StarRocksSinkOptions sinkOptions = new StarRocksSinkOptions(options, context.getCatalogTable().getOptions());

        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new StarRocksDynamicTableSink(sinkOptions, physicalSchema);
    }

    /**
     * 用于 SPI发现时候，选用指定的实现，和'connector'='starrocks' 对应
     * @return
     */
    @Override
    public String factoryIdentifier() {
        return "starrocks";
    }

    /**
     * 必选参数
     * @return
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(StarRocksSinkOptions.JDBC_URL);
        requiredOptions.add(StarRocksSinkOptions.LOAD_URL);
        requiredOptions.add(StarRocksSinkOptions.DATABASE_NAME);
        requiredOptions.add(StarRocksSinkOptions.TABLE_NAME);
        requiredOptions.add(StarRocksSinkOptions.USERNAME);
        requiredOptions.add(StarRocksSinkOptions.PASSWORD);
        return requiredOptions;
    }

    /**
     * 可选参数
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(StarRocksSinkOptions.SINK_VERSION);
        optionalOptions.add(StarRocksSinkOptions.SINK_BATCH_MAX_SIZE);
        optionalOptions.add(StarRocksSinkOptions.SINK_BATCH_MAX_ROWS);
        optionalOptions.add(StarRocksSinkOptions.SINK_BATCH_FLUSH_INTERVAL);
        optionalOptions.add(StarRocksSinkOptions.SINK_MAX_RETRIES);
        optionalOptions.add(StarRocksSinkOptions.SINK_SEMANTIC);
        optionalOptions.add(StarRocksSinkOptions.SINK_BATCH_OFFER_TIMEOUT);
        optionalOptions.add(StarRocksSinkOptions.SINK_PARALLELISM);
        optionalOptions.add(StarRocksSinkOptions.SINK_LABEL_PREFIX);
        optionalOptions.add(StarRocksSinkOptions.SINK_CONNECT_TIMEOUT);
        optionalOptions.add(StarRocksSinkOptions.SINK_WAIT_FOR_CONTINUE_TIMEOUT);
        optionalOptions.add(StarRocksSinkOptions.SINK_IO_THREAD_COUNT);
        optionalOptions.add(StarRocksSinkOptions.SINK_CHUNK_LIMIT);
        optionalOptions.add(StarRocksSinkOptions.SINK_SCAN_FREQUENCY);

        // multi follow cluster props
        optionalOptions.add(StarRocksSinkOptions.MULTI_CLUSTER_OPEN);
        optionalOptions.add(StarRocksSinkOptions.MULTI_CLUSTER_LIST_PROPERTIES);
        optionalOptions.add(StarRocksSinkOptions.IGNORE_MAIN_CLUSTER_FAIL);

        //是否动态落库表,默认false
        optionalOptions.add(StarRocksSinkOptions.SINK_DYNAMIC_DATABASE);
        optionalOptions.add(StarRocksSinkOptions.SINK_DYNAMIC_TABLE);
        //动态落库表 依赖的字段在RowData的中位置,index从0开始
        optionalOptions.add(StarRocksSinkOptions.SINK_DYNAMIC_DATABASE_INDEX);
        optionalOptions.add(StarRocksSinkOptions.SINK_DYNAMIC_TABLE_INDEX);

        //是否关闭schema校验,默认false
        optionalOptions.add(StarRocksSinkOptions.SINK_VALIDATE_TABLE_SCHEMA_CLOSE);

        return optionalOptions;
    }
}
