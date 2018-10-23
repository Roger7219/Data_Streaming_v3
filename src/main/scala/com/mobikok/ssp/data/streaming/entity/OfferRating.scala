package com.mobikok.ssp.data.streaming.entity

case class OfferRating(user: Int,
                       product: Int,
                       rating: Double,
                       importTime: String) {

  def this(rating: Rating, importTime:String) {
    this(rating.user, rating.product, rating.rating, importTime)
  }

}
