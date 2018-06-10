package com.graphdatabases.benchmark;

import com.google.common.base.Stopwatch;
import com.graphdatabases.benchmark.annotation.Benchmark;
import com.graphdatabases.benchmark.annotation.Setup;
import com.graphdatabases.benchmark.annotation.TearDown;
import com.graphdatabases.benchmark.exception.BenchmarkException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BenchmarkTest {

    private Class clazz;

    public BenchmarkTest(Class clazz) {
        this.clazz = clazz;
    }

    public void run() {
        System.out.println(String.format("%s: Benchmark started.", clazz.getName()));

        try {
            Object object = clazz.newInstance();

            List<Method> setupMethods = new ArrayList<>();
            List<Method> tearDownMethods = new ArrayList<>();
            Map<Integer, List<Method>> benchmarkMethodsGroupedByPriority = new HashMap<>();

            Method[] methods = clazz.getMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(Setup.class)) {
                    setupMethods.add(method);
                } else if (method.isAnnotationPresent(Benchmark.class)) {
                    Benchmark benchmark = method.getAnnotation(Benchmark.class);

                    if (benchmarkMethodsGroupedByPriority.containsKey(benchmark.priority())) {
                        benchmarkMethodsGroupedByPriority.get(benchmark.priority()).add(method);
                    } else {
                        List<Method> groupedMethods = new ArrayList<>();
                        groupedMethods.add(method);

                        benchmarkMethodsGroupedByPriority.put(benchmark.priority(), groupedMethods);
                    }
                } else if (method.isAnnotationPresent(TearDown.class)) {
                    tearDownMethods.add(method);
                }
            }
            List<Method> benchmarkMethods = sortBenchmarkMethodsByPriority(benchmarkMethodsGroupedByPriority);

            // setup
            validateSetupMethods(setupMethods);
            for (Method method : setupMethods) {
                System.out.println(String.format("Invoking setup method: %s", method.getName()));
                method.invoke(object);
            }

            // benchmark
            for (Method method : benchmarkMethods) {
                Benchmark benchmark = method.getAnnotation(Benchmark.class);

                List<Long> times = new ArrayList<>();
                for (int i = 0; i < benchmark.iteration(); i++) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    method.invoke(object);
                    stopwatch.stop();

                    times.add(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }

                System.out.println(String.format("%s: Execution times - %s", method.getName(), times));
            }

            // teardown
            validateTearDownMethods(tearDownMethods);
            for (Method method : tearDownMethods) {
                System.out.println(String.format("Invoking tear down method: %s", method.getName()));
                method.invoke(object);
            }

        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
            throw new BenchmarkException(String.format("Failed to run benchmark test. Message: %s", e.getMessage()));
        }

        System.out.println(String.format("%s: Benchmark finished.", clazz.getName()));
    }

    private List<Method> sortBenchmarkMethodsByPriority(Map<Integer, List<Method>> map) {
        List<Method> sortedMethods = new ArrayList<>();

        List<Integer> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys, Collections.reverseOrder());
        for (Integer key : keys) {
            sortedMethods.addAll(map.get(key));
        }

        return sortedMethods;
    }

    private void validateSetupMethods(List<Method> methods) {
        if (methods.size() > 1) {
            throw new BenchmarkException("Only one method can be marked with Setup annotation.");
        }

        if (methods.size() == 1) {
            if (methods.get(0).getParameterCount() > 0) {
                throw new BenchmarkException("Methods marked with Setup annotation cannot have parameters.");
            }
        }
    }

    private void validateTearDownMethods(List<Method> methods) {
        if (methods.size() > 1) {
            throw new BenchmarkException("Only one method can be marked with TearDown annotation.");
        }

        if (methods.size() == 1) {
            if (methods.get(0).getParameterCount() > 0) {
                throw new BenchmarkException("Methods marked with TearDown annotation cannot have parameters.");
            }
        }
    }
}
