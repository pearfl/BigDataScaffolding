package com.pearfl.dlk.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ValueNode;

@FunctionHint(
        input = {@DataTypeHint("STRING"), @DataTypeHint("STRING")},
        output = @DataTypeHint("STRING")
)
public class JsonExtractFunction extends ScalarFunction {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    public String eval(String json, String path) {
        if (json == null || path == null) return null;

        try {
            JsonNode root = mapper.readTree(json);
            JsonNode node = root.at(path);

            if (node.isMissingNode()) return null;
            if (node.isValueNode()) return ((ValueNode) node).asText();
            return node.toString();
        } catch (Exception e) {
            return null; // 返回null触发错误处理管道
        }
    }
}