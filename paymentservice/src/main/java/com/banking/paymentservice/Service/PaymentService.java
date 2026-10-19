package com.banking.paymentservice.Service;

import com.banking.paymentservice.DTO.PaymentRequest;
import com.banking.paymentservice.DTO.PaymentResponse;
import com.banking.paymentservice.Entity.Payment;
import com.banking.paymentservice.Entity.PaymentStatus;
import com.banking.paymentservice.Repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value(("${razorpay.key-secret}"))
    private String keySecret;

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate <String, Object> kafkaTemplate;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    /**
     * create razorpay payment order
     * FLOW:-
     * 1.CREATE ORDER IN RAZORPAY
     * 2.SAVE PAYMENT RECORD IN DB
     * 3. RETURN ORDER DETAILS TO FRONTEND
     * FRONTEND SHOWS RAZORPAY CHECKOUT PAGE
     * 4. USER PAYS
     * 6.RAZORPAY CALLS WEBHOOK
     */

    public PaymentResponse createPaymentOrder(@Valid PaymentRequest request) throws RazorpayException {

        log.info("Creating payment order for account : {} amount: {}", request.getAccountNumber(), request.getAmount());
        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        //CONVERTED AMOUNT
        int convertedAmount = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();
        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount" , convertedAmount);
        orderRequest.put("currency", "USD/INR");
        orderRequest.put("receipt","rcpt_"+ System.currentTimeMillis() + UUID.randomUUID().toString()
                .replace("_","").substring(0,10)
        );

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);
        log.info("RRAZORPAY ORDER CREATED : {} ", razorpayOrder.get("id").toString());

        //SAVE PAYMENT RECORD IN DB
        Payment payment = new Payment();
        payment.setRazorPayOrderId(razorpayOrder.get("id").toString());
        payment.setAmount(request.getAmount());
        payment.setCurrency("USD/INR");
        payment.setAccountNumber(request.getAccountNumber());
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        return new PaymentResponse(
                savedPayment.getId(),
                savedPayment.getRazorPayOrderId(),
                savedPayment.getAmount(),
                "USD/INR",
                "CREATED",
                keyId

        );

    }

    public void handleWebhook(Map<String, Object> payload) {

        log.info("Received razorpay webhook: {}", payload.get("event"));
        String event  = (String) payload.get("event");

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }
        else if("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }

    }

    private void handlePaymentSuccess(Map<String, Object> payload) {
        try{

            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId= paymentData.get("order_id").toString();
            String paymentId= paymentData.get("payment_id").toString();

            Payment payment = paymentRepository.findByRazorPayOrderId(orderId).orElseThrow(() -> new RuntimeException("Payment not found for order "+ orderId));
            payment.setRazorPayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            //PUBLISH PAYMENT COMPLETED EVENT TO KAFKA
            Map<String,Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId",paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,payment.getId(), event);
                    }
        catch (Exception e){
            log.error("Error handling payment success: {}", e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId= paymentData.get("order_id").toString();
            String paymentId= paymentData.get("payment_id").toString();

            Payment payment = paymentRepository.findByRazorPayOrderId(orderId).orElseThrow(() -> new RuntimeException("Payment not found for order "+ orderId));
            payment.setRazorPayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via razorpay");
            paymentRepository.save(payment);

            //PUBLISH PAYMENT COMPLETED EVENT TO KAFKA
            Map<String,Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", "Payment failed via razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId(), event);
            log.warn("payment faield:{}", payment.getId());

        } catch (Exception e) {

            log.error("Error handling payment failure: {}", e.getMessage());
        }
    }

    private Map<String,Object> extractPaymentData(Map<String, Object> payload) {

        Map<String,Object> entity =(Map<String,Object>) payload.get("payload");

        Map<String,Object> paymentwrapper =(Map<String,Object>) entity.get("payment");

        return (Map<String,Object>) paymentwrapper.get("entity");
    }
}
