case class BankTransaction(
  transactionId: String,
  accountId: String,
  customerId: String,
  transactionType: String,
  amount: Double,
  timestamp: String,
  location: String,
  pinStatus: String
)
