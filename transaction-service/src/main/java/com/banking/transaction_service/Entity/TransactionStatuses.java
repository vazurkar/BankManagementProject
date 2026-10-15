package com.banking.transaction_service.Entity;
//pending -> processing -> completed (clean transaction)
//                       ->pending_verification(suspicious detected)
//                            -> completed (verified)
//                            -> flagged(SAGA returned)
//                       ->FAILED
//                       ->FLAGGED


public enum TransactionStatuses {

    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED

}
