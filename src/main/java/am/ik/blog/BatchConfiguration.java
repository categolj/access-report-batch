package am.ik.blog;

import am.ik.blog.entry.EntryProps;
import am.ik.blog.github.GithubProps;
import am.ik.blog.lognroll.LognrollProps;
import am.ik.spring.http.client.RetryableClientHttpRequestInterceptor;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Set;
import org.zalando.logbook.Logbook;
import org.zalando.logbook.spring.LogbookClientHttpRequestInterceptor;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.web.client.RestClient;

@Configuration
public class BatchConfiguration {

	@Bean
	public RestClientCustomizer restClientCustomizer(Logbook logbook) {
		return restClientBuilder -> {
			ExponentialBackOff backOff = new ExponentialBackOff();
			backOff.setMultiplier(2);
			backOff.setMaxElapsedTime(300_000);
			JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory();
			requestFactory.setReadTimeout(Duration.ofSeconds(300));
			restClientBuilder.requestFactory(requestFactory)
				.requestInterceptor(new RetryableClientHttpRequestInterceptor(backOff, Set.of( //
						400 /* SQLITE_BUSY */ , //
						408 /* Request Timeout */, //
						425 /* Too Early */, //
						429 /* Too Many Requests */, //
						500 /* Internal Server Error */, //
						502 /* Bad Gateway */, //
						503 /* Service Unavailable */, //
						504 /* Gateway Timeout */
			)))
				.requestInterceptor(new LogbookClientHttpRequestInterceptor(logbook));
		};
	}

	@Bean
	@StepScope
	public CounterItemReader itemReader(RestClient.Builder builder,
			@Value("#{jobParameters['reportStartDate']}") LocalDate jobStartDate, LognrollProps lognrollProps) {
		return new CounterItemReader(builder.baseUrl(lognrollProps.apiUrl())
			.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + lognrollProps.accessToken()), jobStartDate);
	}

	@Bean
	public CounterItemProcessor itemProcessor() {
		return new CounterItemProcessor();
	}

	@Bean
	@StepScope
	public CounterItemWriter itemWriter(RestClient.Builder githubClientBuilder, RestClient.Builder entryClientBuilder,
			@Value("#{jobParameters['reportStartDate']}") LocalDate reportStartDate, GithubProps githubProps,
			EntryProps entryProps) {
		return new CounterItemWriter(
				githubClientBuilder.baseUrl(githubProps.apiUrl())
					.defaultHeader(HttpHeaders.ACCEPT, "application/vnd.github+json")
					.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + githubProps.accessToken())
					.defaultHeader("X-GitHub-Api-Version", "2022-11-28"),
				entryClientBuilder.baseUrl(entryProps.apiUrl()), reportStartDate);
	}

	@Bean
	public Job accessCounterReportJob(JobRepository jobRepository, Step step1,
			JobCompletionNotificationListener listener) {
		return new JobBuilder("accessCounterReportJob", jobRepository).listener(listener).start(step1).build();
	}

	@Bean
	public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
			CounterItemReader reader, CounterItemProcessor itemProcessor, CounterItemWriter itemWriter) {
		return new StepBuilder("step1", jobRepository).<JsonNode, CounterItem>chunk(1_000_000, transactionManager)
			.reader(reader)
			.processor(itemProcessor)
			.writer(itemWriter)
			.build();
	}

}