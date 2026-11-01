package com.banking.transactionservice.entity;

public enum TransactionStatus {
    /**
     * transaction lifecycle flow
     * pending->processing->completed(clear transaction)
     *                     -->PENDING VERIFICATION(SUSPICIOUS DETECTED)
     *                                  -->COMPLETED(OTP)
     *                      -->FAILED
     *                      -->FLAGGED(SAGA REFUND)
     *
     *
     */
    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    SUCCESS,
    FLAGGED

}
