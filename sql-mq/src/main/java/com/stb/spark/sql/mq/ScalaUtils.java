package com.stb.spark.sql.mq;

import scala.runtime.AbstractFunction0;

public class ScalaUtils {
    private ScalaUtils() {
    }

    public static class DefaultStringFunction extends AbstractFunction0<String> implements scala.Serializable {
        private final String defaultValue;
        public DefaultStringFunction(String defaultValue) {
            this.defaultValue = defaultValue;
        }
        @Override
        public String apply() {
            return defaultValue;
        }
    }

    public static class DefaultNullFunction extends AbstractFunction0<String> implements scala.Serializable {
        @Override
        public String apply() {
            return null;
        }
    }

    public static class DefaultTrueFunction extends AbstractFunction0<Boolean> implements scala.Serializable {
        @Override
        public Boolean apply() {
            return true;
        }
    }

    public static class DefaultEmptyFunction extends AbstractFunction0<String> implements scala.Serializable {
        @Override
        public String apply() {
            return "";
        }
    }

    public static class DefaultZeroFunction extends AbstractFunction0<String> implements scala.Serializable {
        @Override
        public String apply() {
            return "0";
        }
    }
}